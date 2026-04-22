// Supported usage:
//   1) bgp_simulator --relationships <graph_file> --announcements <anns_csv>
//                    --rov-asns <rov_asns_csv>
//      Writes ribs.csv to the current directory.
//   2) bgp_sim <dataset_dir> <output_csv>
//   3) bgp_sim <graph_file> <anns_csv> <rov_asns_csv> <output_csv>
//
// The simulator:
// - builds the AS graph from CAIDA relationships
// - detects provider/customer cycles
// - seeds announcements
// - propagates them using the project-specified up / across / down phases
// - applies BGP tie-breaking and optional ROV drops
// - writes ribs.csv-compatible output

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr int kCycleExitCode = 2;
constexpr std::size_t kIoBufferSize = 8u << 20;

enum class Relationship : std::uint8_t {
    Provider = 1,
    Peer = 2,
    Customer = 3,
    Origin = 4,
};

struct Node {
    std::uint32_t asn = 0;
    std::uint32_t providers_offset = 0;
    std::uint32_t providers_count = 0;
    std::uint32_t customers_offset = 0;
    std::uint32_t customers_count = 0;
    std::uint32_t peers_offset = 0;
    std::uint32_t peers_count = 0;
    bool rov_enabled = false;
};

struct SeedAnnouncement {
    std::uint32_t node_index = 0;
    bool rov_invalid = false;
};

struct PrefixWorkItem {
    std::string csv_row_prefix;
    std::vector<SeedAnnouncement> seeds;
};

struct RouteRecord {
    std::int32_t parent_route = -1;
    std::uint32_t node_index = 0;
};

struct PendingRoute {
    std::int32_t parent_route = -1;
    std::uint32_t next_hop_asn = 0;
    std::uint32_t path_length = 0;
    Relationship relationship = Relationship::Provider;
    bool rov_invalid = false;
    std::uint32_t epoch = 0;
};

struct BestRouteSlot {
    std::int32_t route_id = -1;
    std::uint32_t epoch = 0;
    std::uint32_t next_hop_asn = 0;
    std::uint32_t path_length = 0;
    Relationship relationship = Relationship::Provider;
    bool rov_invalid = false;
};

struct alignas(64) WorkerResult {
    std::string output;
};

struct RouteArena {
    std::vector<RouteRecord> storage;
    std::uint32_t size = 0;

    void initialize(std::size_t capacity) {
        storage.clear();
        storage.reserve(capacity);
        size = 0;
    }

    void reset() { size = 0; }

    [[nodiscard]] std::size_t capacity() const { return storage.capacity(); }

    std::int32_t push(const RouteRecord& route) {
        if (size >= storage.capacity()) {
            throw std::runtime_error("Route arena capacity exceeded.");
        }
        if (size < storage.size()) {
            storage[size] = route;
        } else {
            storage.push_back(route);
        }
        return static_cast<std::int32_t>(size++);
    }

    [[nodiscard]] const RouteRecord& operator[](std::size_t index) const {
        return storage[index];
    }
};

struct InputPaths {
    fs::path graph_path;
    fs::path anns_path;
    fs::path rov_path;
    fs::path output_path;
};

struct IndexView {
    const std::uint32_t* data = nullptr;
    std::size_t size = 0;

    [[nodiscard]] const std::uint32_t* begin() const { return data; }
    [[nodiscard]] const std::uint32_t* end() const {
        return data == nullptr ? nullptr : data + size;
    }
};

struct WorkerAssignment {
    std::size_t begin_index = 0;
    std::size_t end_index = 0;
};

class Graph {
public:
    void reserve_nodes(std::size_t count) {
        nodes_.reserve(count);
        asn_texts_.reserve(count);
        asn_path_fragments_.reserve(count);
        provider_lists_.reserve(count);
        customer_lists_.reserve(count);
        peer_lists_.reserve(count);
    }

    std::uint32_t ensure_node(std::uint32_t asn) {
        const auto it = index_by_asn_.find(asn);
        if (it != index_by_asn_.end()) {
            return it->second;
        }

        const std::uint32_t index = static_cast<std::uint32_t>(nodes_.size());
        index_by_asn_.emplace(asn, index);
        const std::string asn_text = std::to_string(asn);
        Node node;
        node.asn = asn;
        nodes_.push_back(std::move(node));
        asn_texts_.push_back(asn_text);
        asn_path_fragments_.push_back(", " + asn_text);
        provider_lists_.emplace_back();
        customer_lists_.emplace_back();
        peer_lists_.emplace_back();
        return index;
    }

    [[nodiscard]] std::uint32_t index_for_asn(std::uint32_t asn) const {
        return index_by_asn_.at(asn);
    }

    [[nodiscard]] bool has_asn(std::uint32_t asn) const {
        return index_by_asn_.find(asn) != index_by_asn_.end();
    }

    [[nodiscard]] std::vector<Node>& nodes() { return nodes_; }
    [[nodiscard]] const std::vector<Node>& nodes() const { return nodes_; }

    [[nodiscard]] std::size_t node_count() const { return nodes_.size(); }

    [[nodiscard]] const std::string& asn_text(std::uint32_t node_index) const {
        return asn_texts_[node_index];
    }

    [[nodiscard]] const std::string& asn_path_fragment(
        std::uint32_t node_index) const {
        return asn_path_fragments_[node_index];
    }

    void add_provider_customer_edge(std::uint32_t provider_index,
                                    std::uint32_t customer_index) {
        customer_lists_[provider_index].push_back(customer_index);
        provider_lists_[customer_index].push_back(provider_index);
    }

    void add_peer_edge(std::uint32_t left_index, std::uint32_t right_index) {
        peer_lists_[left_index].push_back(right_index);
        peer_lists_[right_index].push_back(left_index);
    }

    bool build_propagation_ranks(std::ostream& err) {
        const std::size_t n = nodes_.size();
        rank_offsets_.clear();
        rank_nodes_.clear();
        if (n == 0) {
            return true;
        }

        std::vector<std::uint32_t> remaining_customers(n, 0);
        std::vector<std::uint32_t> propagation_rank(n, 0);
        std::vector<std::uint32_t> queue;
        queue.reserve(n);

        for (std::uint32_t i = 0; i < n; ++i) {
            remaining_customers[i] =
                static_cast<std::uint32_t>(customer_lists_[i].size());
            if (remaining_customers[i] == 0) {
                queue.push_back(i);
            }
        }

        std::size_t head = 0;
        std::size_t visited = 0;
        std::uint32_t max_rank = 0;

        while (head < queue.size()) {
            const std::uint32_t node_index = queue[head++];
            ++visited;
            const std::uint32_t node_rank = propagation_rank[node_index];
            max_rank = std::max(max_rank, node_rank);

            for (const std::uint32_t provider_index :
                 provider_lists_[node_index]) {
                propagation_rank[provider_index] =
                    std::max(propagation_rank[provider_index], node_rank + 1);
                if (--remaining_customers[provider_index] == 0) {
                    queue.push_back(provider_index);
                }
            }
        }

        if (visited != n) {
            err << "Cycle detected in provider/customer relationships.\n";
            return false;
        }

        rank_offsets_.assign(static_cast<std::size_t>(max_rank) + 2, 0);
        for (std::uint32_t i = 0; i < n; ++i) {
            ++rank_offsets_[static_cast<std::size_t>(propagation_rank[i]) + 1];
        }
        for (std::size_t i = 1; i < rank_offsets_.size(); ++i) {
            rank_offsets_[i] += rank_offsets_[i - 1];
        }

        rank_nodes_.resize(n);
        std::vector<std::uint32_t> write_offsets = rank_offsets_;
        for (std::uint32_t i = 0; i < n; ++i) {
            const std::size_t rank = propagation_rank[i];
            rank_nodes_[write_offsets[rank]++] = i;
        }
        return true;
    }

    void deduplicate_adjacency() {
        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            auto dedupe = [](std::vector<std::uint32_t>& neighbors) {
                std::sort(neighbors.begin(), neighbors.end());
                neighbors.erase(std::unique(neighbors.begin(), neighbors.end()),
                                neighbors.end());
            };
            dedupe(provider_lists_[i]);
            dedupe(customer_lists_[i]);
            dedupe(peer_lists_[i]);
        }
    }

    void freeze_adjacency() {
        std::size_t provider_total = 0;
        std::size_t customer_total = 0;
        std::size_t peer_total = 0;
        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            provider_total += provider_lists_[i].size();
            customer_total += customer_lists_[i].size();
            peer_total += peer_lists_[i].size();
        }

        provider_edges_.resize(provider_total);
        customer_edges_.resize(customer_total);
        peer_edges_.resize(peer_total);

        std::size_t provider_cursor = 0;
        std::size_t customer_cursor = 0;
        std::size_t peer_cursor = 0;

        for (std::size_t i = 0; i < nodes_.size(); ++i) {
            Node& node = nodes_[i];

            node.providers_offset = static_cast<std::uint32_t>(provider_cursor);
            node.providers_count =
                static_cast<std::uint32_t>(provider_lists_[i].size());
            std::copy(provider_lists_[i].begin(), provider_lists_[i].end(),
                      provider_edges_.begin() + static_cast<std::ptrdiff_t>(provider_cursor));
            provider_cursor += provider_lists_[i].size();

            node.customers_offset = static_cast<std::uint32_t>(customer_cursor);
            node.customers_count =
                static_cast<std::uint32_t>(customer_lists_[i].size());
            std::copy(customer_lists_[i].begin(), customer_lists_[i].end(),
                      customer_edges_.begin() + static_cast<std::ptrdiff_t>(customer_cursor));
            customer_cursor += customer_lists_[i].size();

            node.peers_offset = static_cast<std::uint32_t>(peer_cursor);
            node.peers_count =
                static_cast<std::uint32_t>(peer_lists_[i].size());
            std::copy(peer_lists_[i].begin(), peer_lists_[i].end(),
                      peer_edges_.begin() + static_cast<std::ptrdiff_t>(peer_cursor));
            peer_cursor += peer_lists_[i].size();
        }

        std::vector<std::vector<std::uint32_t>>().swap(provider_lists_);
        std::vector<std::vector<std::uint32_t>>().swap(customer_lists_);
        std::vector<std::vector<std::uint32_t>>().swap(peer_lists_);
    }

    [[nodiscard]] IndexView providers(std::uint32_t node_index) const {
        const Node& node = nodes_[node_index];
        return make_view(provider_edges_, node.providers_offset, node.providers_count);
    }

    [[nodiscard]] IndexView customers(std::uint32_t node_index) const {
        const Node& node = nodes_[node_index];
        return make_view(customer_edges_, node.customers_offset, node.customers_count);
    }

    [[nodiscard]] IndexView peers(std::uint32_t node_index) const {
        const Node& node = nodes_[node_index];
        return make_view(peer_edges_, node.peers_offset, node.peers_count);
    }

    [[nodiscard]] std::size_t rank_count() const {
        return rank_offsets_.empty() ? 0 : rank_offsets_.size() - 1;
    }

    [[nodiscard]] IndexView rank_nodes(std::size_t rank_index) const {
        const std::uint32_t begin = rank_offsets_[rank_index];
        const std::uint32_t end = rank_offsets_[rank_index + 1];
        return make_view(rank_nodes_, begin, end - begin);
    }

private:
    [[nodiscard]] static IndexView make_view(
        const std::vector<std::uint32_t>& storage, std::uint32_t offset,
        std::uint32_t count) {
        if (count == 0) {
            return {};
        }
        return IndexView{storage.data() + offset, count};
    }

    std::unordered_map<std::uint32_t, std::uint32_t> index_by_asn_;
    std::vector<Node> nodes_;
    std::vector<std::string> asn_texts_;
    std::vector<std::string> asn_path_fragments_;
    std::vector<std::vector<std::uint32_t>> provider_lists_;
    std::vector<std::vector<std::uint32_t>> customer_lists_;
    std::vector<std::vector<std::uint32_t>> peer_lists_;
    std::vector<std::uint32_t> provider_edges_;
    std::vector<std::uint32_t> customer_edges_;
    std::vector<std::uint32_t> peer_edges_;
    std::vector<std::uint32_t> rank_offsets_;
    std::vector<std::uint32_t> rank_nodes_;
};

[[nodiscard]] std::string_view trim_view(std::string_view value) {
    std::size_t start = 0;
    std::size_t end = value.size();
    while (start < end &&
           (value[start] == ' ' || value[start] == '\t' || value[start] == '\r' ||
            value[start] == '\n')) {
        ++start;
    }
    while (end > start &&
           (value[end - 1] == ' ' || value[end - 1] == '\t' ||
            value[end - 1] == '\r' || value[end - 1] == '\n')) {
        --end;
    }
    return value.substr(start, end - start);
}

[[nodiscard]] bool parse_u32(std::string_view text, std::uint32_t& out) {
    const std::string_view cleaned = trim_view(text);
    if (cleaned.empty()) {
        return false;
    }
    const char* begin = cleaned.data();
    const char* end = cleaned.data() + cleaned.size();
    auto [ptr, ec] = std::from_chars(begin, end, out);
    return ec == std::errc{} && ptr == end;
}

[[nodiscard]] bool parse_bool_csv(std::string_view text, bool& out) {
    const std::string_view cleaned = trim_view(text);
    if (cleaned == "True" || cleaned == "true" || cleaned == "1") {
        out = true;
        return true;
    }
    if (cleaned == "False" || cleaned == "false" || cleaned == "0") {
        out = false;
        return true;
    }
    return false;
}

bool better_route(Relationship candidate_relationship,
                  std::uint32_t candidate_path_length,
                  std::uint32_t candidate_next_hop_asn, bool has_current,
                  Relationship current_relationship,
                  std::uint32_t current_path_length,
                  std::uint32_t current_next_hop_asn) {
    if (!has_current) {
        return true;
    }

    if (candidate_relationship != current_relationship) {
        return candidate_relationship > current_relationship;
    }
    if (candidate_path_length != current_path_length) {
        return candidate_path_length < current_path_length;
    }
    if (candidate_next_hop_asn != current_next_hop_asn) {
        return candidate_next_hop_asn < current_next_hop_asn;
    }
    return false;
}

void append_route_csv_row(const Graph& graph, std::uint32_t node_index,
                          const PrefixWorkItem& work_item,
                          std::int32_t route_id,
                          const RouteArena& arena,
                          std::string& output) {
    const std::string& asn_text = graph.asn_text(node_index);
    output.append(asn_text);
    output.append(work_item.csv_row_prefix);

    output.append(asn_text);
    std::int32_t current = arena[static_cast<std::size_t>(route_id)].parent_route;
    if (current < 0) {
        output.push_back(',');
    }
    while (current >= 0) {
        const RouteRecord& route = arena[static_cast<std::size_t>(current)];
        output.append(graph.asn_path_fragment(route.node_index));
        current = route.parent_route;
    }

    output.push_back(')');
    output.push_back('"');
    output.push_back('\n');
}

[[nodiscard]] InputPaths resolve_input_paths(int argc, char** argv) {
    if (argc > 1 && std::string_view(argv[1]).rfind("--", 0) == 0) {
        InputPaths paths;
        paths.output_path = fs::current_path() / "ribs.csv";

        for (int i = 1; i < argc; ++i) {
            const std::string_view arg = argv[i];
            auto require_value = [&](std::string_view flag) -> fs::path {
                if (i + 1 >= argc) {
                    throw std::runtime_error("Missing value for " + std::string(flag));
                }
                ++i;
                return argv[i];
            };

            if (arg == "--relationships") {
                paths.graph_path = require_value(arg);
            } else if (arg == "--announcements") {
                paths.anns_path = require_value(arg);
            } else if (arg == "--rov-asns") {
                paths.rov_path = require_value(arg);
            } else if (arg == "--output") {
                paths.output_path = require_value(arg);
            } else {
                throw std::runtime_error("Unknown argument: " + std::string(arg));
            }
        }

        if (paths.graph_path.empty() || paths.anns_path.empty() ||
            paths.rov_path.empty()) {
            throw std::runtime_error(
                "Usage:\n"
                "  bgp_simulator --relationships <graph_file> --announcements "
                "<anns_csv> --rov-asns <rov_asns_csv>\n"
                "Optional:\n"
                "  --output <output_csv>  (defaults to ./ribs.csv)");
        }
        return paths;
    }

    if (argc == 3) {
        const fs::path dataset_dir = argv[1];
        std::vector<fs::path> graph_candidates;
        for (const auto& entry : fs::directory_iterator(dataset_dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const std::string filename = entry.path().filename().string();
            if (filename.rfind("CAIDAASGraphCollector_", 0) == 0 &&
                entry.path().extension() == ".txt") {
                graph_candidates.push_back(entry.path());
            }
        }
        std::sort(graph_candidates.begin(), graph_candidates.end());
        if (graph_candidates.empty()) {
            throw std::runtime_error(
                "No CAIDAASGraphCollector_*.txt file found in dataset directory.");
        }
        return InputPaths{graph_candidates.front(), dataset_dir / "anns.csv",
                          dataset_dir / "rov_asns.csv", argv[2]};
    }

    if (argc == 5) {
        return InputPaths{argv[1], argv[2], argv[3], argv[4]};
    }

    throw std::runtime_error(
        "Usage:\n"
        "  bgp_simulator --relationships <graph_file> --announcements "
        "<anns_csv> --rov-asns <rov_asns_csv>\n"
        "  (writes ribs.csv in the current directory unless --output is set)\n"
        "  bgp_sim <dataset_dir> <output_csv>\n"
        "  bgp_sim <graph_file> <anns_csv> <rov_asns_csv> <output_csv>");
}

void load_graph_file(const fs::path& graph_path, Graph& graph) {
    std::ifstream input(graph_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Failed to open graph file: " +
                                 graph_path.string());
    }
    std::vector<char> buffer(kIoBufferSize);
    input.rdbuf()->pubsetbuf(buffer.data(),
                             static_cast<std::streamsize>(buffer.size()));

    graph.reserve_nodes(131072);
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }

        const std::size_t p1 = line.find('|');
        if (p1 == std::string::npos) {
            continue;
        }
        const std::size_t p2 = line.find('|', p1 + 1);
        if (p2 == std::string::npos) {
            continue;
        }
        const std::size_t p3 = line.find('|', p2 + 1);

        std::uint32_t left_asn = 0;
        std::uint32_t right_asn = 0;
        if (!parse_u32(std::string_view(line).substr(0, p1), left_asn) ||
            !parse_u32(std::string_view(line).substr(p1 + 1, p2 - p1 - 1),
                       right_asn)) {
            continue;
        }

        const std::string_view relation_text = trim_view(
            p3 == std::string::npos
                ? std::string_view(line).substr(p2 + 1)
                : std::string_view(line).substr(p2 + 1, p3 - p2 - 1));

        const std::uint32_t left_index = graph.ensure_node(left_asn);
        const std::uint32_t right_index = graph.ensure_node(right_asn);
        if (relation_text == "-1") {
            graph.add_provider_customer_edge(left_index, right_index);
        } else if (relation_text == "0") {
            graph.add_peer_edge(left_index, right_index);
        }
    }
}

std::vector<std::uint32_t> load_rov_file(const fs::path& rov_path,
                                         Graph& graph) {
    std::ifstream input(rov_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Failed to open ROV ASN file: " +
                                 rov_path.string());
    }
    std::vector<char> buffer(kIoBufferSize);
    input.rdbuf()->pubsetbuf(buffer.data(),
                             static_cast<std::streamsize>(buffer.size()));

    std::vector<std::uint32_t> rov_nodes;
    std::string line;
    while (std::getline(input, line)) {
        const std::string_view cleaned = trim_view(line);
        if (cleaned.empty()) {
            continue;
        }
        std::uint32_t asn = 0;
        if (!parse_u32(cleaned, asn)) {
            continue;
        }
        rov_nodes.push_back(graph.ensure_node(asn));
    }
    return rov_nodes;
}

std::vector<PrefixWorkItem> load_announcements_file(const fs::path& anns_path,
                                                    Graph& graph) {
    std::ifstream input(anns_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Failed to open announcements file: " +
                                 anns_path.string());
    }
    std::vector<char> buffer(kIoBufferSize);
    input.rdbuf()->pubsetbuf(buffer.data(),
                             static_cast<std::streamsize>(buffer.size()));

    std::vector<PrefixWorkItem> work_items;
    std::unordered_map<std::string, std::size_t> prefix_index;
    prefix_index.reserve(128);

    std::string line;
    bool header_skipped = false;
    while (std::getline(input, line)) {
        if (!header_skipped) {
            header_skipped = true;
            if (line.find("seed_asn") != std::string::npos) {
                continue;
            }
        }
        if (line.empty()) {
            continue;
        }

        const std::size_t c1 = line.find(',');
        if (c1 == std::string::npos) {
            continue;
        }
        const std::size_t c2 = line.find(',', c1 + 1);
        if (c2 == std::string::npos) {
            continue;
        }

        std::uint32_t seed_asn = 0;
        if (!parse_u32(std::string_view(line).substr(0, c1), seed_asn)) {
            continue;
        }
        const std::string_view prefix_view =
            trim_view(std::string_view(line).substr(c1 + 1, c2 - c1 - 1));
        bool rov_invalid = false;
        if (!parse_bool_csv(std::string_view(line).substr(c2 + 1), rov_invalid)) {
            continue;
        }
        const std::string prefix(prefix_view);

        const std::uint32_t node_index = graph.ensure_node(seed_asn);
        const auto [it, inserted] =
            prefix_index.emplace(prefix, work_items.size());
        if (inserted) {
            PrefixWorkItem item;
            item.csv_row_prefix.reserve(prefix.size() + 5);
            item.csv_row_prefix.push_back(',');
            item.csv_row_prefix.append(prefix);
            item.csv_row_prefix.append(",\"(");
            work_items.push_back(std::move(item));
        }
        SeedAnnouncement seed;
        seed.node_index = node_index;
        seed.rov_invalid = rov_invalid;
        work_items[it->second].seeds.push_back(seed);
    }

    return work_items;
}

void maybe_store_pending_route(std::vector<PendingRoute>& pending_for_node,
                               std::uint32_t current_epoch,
                               std::uint32_t destination_node,
                               std::int32_t parent_route,
                               std::uint32_t next_hop_asn,
                               std::uint32_t path_length,
                               Relationship relationship, bool rov_invalid,
                               std::vector<std::uint32_t>* activated_nodes = nullptr) {
    PendingRoute& pending = pending_for_node[destination_node];
    if (pending.epoch != current_epoch) {
        if (activated_nodes != nullptr) {
            activated_nodes->push_back(destination_node);
        }
        pending.parent_route = parent_route;
        pending.next_hop_asn = next_hop_asn;
        pending.path_length = path_length;
        pending.relationship = relationship;
        pending.rov_invalid = rov_invalid;
        pending.epoch = current_epoch;
        return;
    }

    if (better_route(relationship, path_length, next_hop_asn, true,
                     pending.relationship, pending.path_length,
                     pending.next_hop_asn)) {
        pending.parent_route = parent_route;
        pending.next_hop_asn = next_hop_asn;
        pending.path_length = path_length;
        pending.relationship = relationship;
        pending.rov_invalid = rov_invalid;
        pending.epoch = current_epoch;
    }
}

void simulate_prefix(const Graph& graph, const PrefixWorkItem& work_item,
                     std::uint32_t current_epoch,
                     std::vector<BestRouteSlot>& best_route_for_node,
                     std::vector<PendingRoute>& pending_for_node,
                     RouteArena& arena,
                     std::vector<std::uint32_t>& installed_nodes,
                     std::vector<std::uint32_t>& peer_pending_nodes,
                     std::string& output) {
    arena.reset();
    const Node* const node_data = graph.nodes().data();
    const std::uint32_t node_count =
        static_cast<std::uint32_t>(graph.node_count());
    const std::size_t rank_count = graph.rank_count();
    installed_nodes.clear();
    peer_pending_nodes.clear();

    auto process_inbox = [&](std::uint32_t node_index) {
        PendingRoute& pending = pending_for_node[node_index];
        if (pending.epoch != current_epoch) {
            return;
        }

        BestRouteSlot& best_slot = best_route_for_node[node_index];
        const bool has_best = best_slot.epoch == current_epoch;
        if (!has_best ||
            better_route(pending.relationship, pending.path_length,
                         pending.next_hop_asn, true, best_slot.relationship,
                         best_slot.path_length, best_slot.next_hop_asn)) {
            if (!has_best) {
                installed_nodes.push_back(node_index);
            }
            best_slot.route_id =
                arena.push(RouteRecord{pending.parent_route, node_index});
            best_slot.epoch = current_epoch;
            best_slot.next_hop_asn = pending.next_hop_asn;
            best_slot.path_length = pending.path_length;
            best_slot.relationship = pending.relationship;
            best_slot.rov_invalid = pending.rov_invalid;
        }

        pending.epoch = 0;
    };

    auto send_to_neighbors = [&](std::uint32_t node_index,
                                 IndexView neighbors,
                                 Relationship received_relationship,
                                 std::vector<std::uint32_t>* activated_nodes = nullptr) {
        const BestRouteSlot& best_slot = best_route_for_node[node_index];
        if (best_slot.epoch != current_epoch || neighbors.size == 0) {
            return;
        }
        const std::int32_t route_id = best_slot.route_id;
        const std::uint32_t sender_asn = node_data[node_index].asn;
        const std::uint32_t candidate_path_length = best_slot.path_length + 1;
        const std::uint32_t* neighbor = neighbors.data;
        const std::uint32_t* const neighbor_end = neighbor + neighbors.size;
        if (!best_slot.rov_invalid) {
            for (; neighbor != neighbor_end; ++neighbor) {
                const std::uint32_t neighbor_index = *neighbor;
                maybe_store_pending_route(pending_for_node, current_epoch,
                                          neighbor_index, route_id, sender_asn,
                                          candidate_path_length,
                                          received_relationship, false,
                                          activated_nodes);
            }
            return;
        }
        for (; neighbor != neighbor_end; ++neighbor) {
            const std::uint32_t neighbor_index = *neighbor;
            if (node_data[neighbor_index].rov_enabled) {
                continue;
            }
            maybe_store_pending_route(pending_for_node, current_epoch,
                                      neighbor_index, route_id, sender_asn,
                                      candidate_path_length,
                                      received_relationship, true,
                                      activated_nodes);
        }
    };

    for (const SeedAnnouncement& seed : work_item.seeds) {
        const std::int32_t route_id = arena.push(RouteRecord{-1, seed.node_index});
        BestRouteSlot& best_slot = best_route_for_node[seed.node_index];
        const bool has_current = best_slot.epoch == current_epoch;
        const std::uint32_t origin_asn = node_data[seed.node_index].asn;
        if (!has_current ||
            better_route(Relationship::Origin, 1, origin_asn, true,
                         best_slot.relationship, best_slot.path_length,
                         best_slot.next_hop_asn)) {
            if (!has_current) {
                installed_nodes.push_back(seed.node_index);
            }
            best_slot.route_id = route_id;
            best_slot.epoch = current_epoch;
            best_slot.next_hop_asn = origin_asn;
            best_slot.path_length = 1;
            best_slot.relationship = Relationship::Origin;
            best_slot.rov_invalid = seed.rov_invalid;
        }
    }

    for (std::size_t rank_index = 0; rank_index < rank_count; ++rank_index) {
        const IndexView rank_nodes = graph.rank_nodes(rank_index);
        if (rank_nodes.size != 0) {
            const std::uint32_t* node = rank_nodes.data;
            const std::uint32_t* const node_end = node + rank_nodes.size;
            for (; node != node_end; ++node) {
                process_inbox(*node);
            }
            node = rank_nodes.data;
            for (; node != node_end; ++node) {
                send_to_neighbors(*node, graph.providers(*node),
                                  Relationship::Customer);
            }
        }
    }

    for (const std::uint32_t node_index : installed_nodes) {
        send_to_neighbors(node_index, graph.peers(node_index), Relationship::Peer,
                          &peer_pending_nodes);
    }
    for (const std::uint32_t node_index : peer_pending_nodes) {
        process_inbox(node_index);
    }

    for (std::size_t rank_index = rank_count; rank_index > 0; --rank_index) {
        const IndexView rank_nodes = graph.rank_nodes(rank_index - 1);
        if (rank_nodes.size != 0) {
            const std::uint32_t* node = rank_nodes.data;
            const std::uint32_t* const node_end = node + rank_nodes.size;
            for (; node != node_end; ++node) {
                process_inbox(*node);
            }
            node = rank_nodes.data;
            for (; node != node_end; ++node) {
                send_to_neighbors(*node, graph.customers(*node),
                                  Relationship::Provider);
            }
        }
    }

    for (std::uint32_t node_index = 0; node_index < node_count; ++node_index) {
        const BestRouteSlot& best_slot = best_route_for_node[node_index];
        if (best_slot.epoch == current_epoch) {
            append_route_csv_row(graph, node_index, work_item,
                                 best_slot.route_id, arena, output);
        }
    }
}

[[nodiscard]] std::vector<WorkerAssignment> build_worker_assignments(
    const std::vector<PrefixWorkItem>& work_items, std::size_t thread_count) {
    std::vector<WorkerAssignment> assignments(thread_count);
    if (thread_count == 0) {
        return assignments;
    }
    if (thread_count == 1 || work_items.empty()) {
        assignments[0] = WorkerAssignment{0, work_items.size()};
        return assignments;
    }

    std::uint64_t total_cost = 0;
    for (const PrefixWorkItem& work_item : work_items) {
        total_cost += static_cast<std::uint64_t>(work_item.seeds.size());
    }

    std::size_t best_split = 1;
    std::uint64_t left_cost = 0;
    std::uint64_t best_imbalance = std::numeric_limits<std::uint64_t>::max();
    for (std::size_t split = 1; split < work_items.size(); ++split) {
        left_cost += static_cast<std::uint64_t>(work_items[split - 1].seeds.size());
        const std::uint64_t right_cost = total_cost - left_cost;
        const std::uint64_t imbalance =
            left_cost > right_cost ? left_cost - right_cost : right_cost - left_cost;
        if (imbalance < best_imbalance) {
            best_imbalance = imbalance;
            best_split = split;
        }
    }

    assignments[0] = WorkerAssignment{0, best_split};
    assignments[1] = WorkerAssignment{best_split, work_items.size()};
    return assignments;
}

void run_worker(const Graph& graph, const std::vector<PrefixWorkItem>& work_items,
                std::size_t begin_index, std::size_t end_index,
                WorkerResult& result) {
    const std::size_t node_count = graph.node_count();
    std::vector<BestRouteSlot> best_route_for_node(node_count);
    std::vector<PendingRoute> pending_for_node(node_count);
    std::vector<std::uint32_t> installed_nodes;
    std::vector<std::uint32_t> peer_pending_nodes;
    RouteArena arena;
    std::uint32_t current_epoch = 1;
    installed_nodes.reserve(node_count);
    peer_pending_nodes.reserve(node_count);

    const std::size_t prefix_count = end_index - begin_index;
    std::size_t max_seed_count = 0;
    for (std::size_t i = begin_index; i < end_index; ++i) {
        max_seed_count = std::max(max_seed_count, work_items[i].seeds.size());
    }
    arena.initialize(node_count + max_seed_count + 1024);
    result.output.clear();
    result.output.reserve(prefix_count * node_count * 48);

    for (std::size_t i = begin_index; i < end_index; ++i) {
        simulate_prefix(graph, work_items[i], current_epoch, best_route_for_node,
                        pending_for_node, arena, installed_nodes,
                        peer_pending_nodes, result.output);
        ++current_epoch;
    }
}

void write_output_file(const fs::path& output_path,
                       const std::vector<WorkerResult>& worker_results) {
    if (output_path.has_parent_path()) {
        fs::create_directories(output_path.parent_path());
    }

    std::ofstream output(output_path, std::ios::binary);
    if (!output) {
        throw std::runtime_error("Failed to open output file: " +
                                 output_path.string());
    }
    std::vector<char> buffer(kIoBufferSize);
    output.rdbuf()->pubsetbuf(buffer.data(),
                              static_cast<std::streamsize>(buffer.size()));

    output << "asn,prefix,as_path\n";
    for (const WorkerResult& worker_result : worker_results) {
        output.write(worker_result.output.data(),
                     static_cast<std::streamsize>(worker_result.output.size()));
    }
}

}  // namespace

int main(int argc, char** argv) {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    try {
        const InputPaths paths = resolve_input_paths(argc, argv);

        Graph graph;
        load_graph_file(paths.graph_path, graph);
        std::vector<PrefixWorkItem> work_items =
            load_announcements_file(paths.anns_path, graph);
        const std::vector<std::uint32_t> rov_nodes =
            load_rov_file(paths.rov_path, graph);

        for (const std::uint32_t node_index : rov_nodes) {
            graph.nodes()[node_index].rov_enabled = true;
        }
        graph.deduplicate_adjacency();

        if (!graph.build_propagation_ranks(std::cerr)) {
            return kCycleExitCode;
        }
        graph.freeze_adjacency();

        std::size_t thread_count = 1;
        if (!work_items.empty()) {
            const unsigned hw = std::thread::hardware_concurrency();
            const std::size_t cpu_cap =
                std::max<std::size_t>(1, std::min<std::size_t>(2, hw == 0 ? 2 : hw));
            thread_count =
                std::max<std::size_t>(1, std::min<std::size_t>(cpu_cap, work_items.size()));
        }

        const std::vector<WorkerAssignment> assignments =
            build_worker_assignments(work_items, thread_count);
        std::vector<WorkerResult> worker_results(thread_count);
        std::vector<std::thread> threads;
        threads.reserve(thread_count > 0 ? thread_count - 1 : 0);

        for (std::size_t worker_index = 0; worker_index < thread_count; ++worker_index) {
            const std::size_t begin_index = assignments[worker_index].begin_index;
            const std::size_t end_index = assignments[worker_index].end_index;

            if (worker_index + 1 == thread_count) {
                run_worker(graph, work_items, begin_index, end_index,
                           worker_results[worker_index]);
            } else {
                threads.emplace_back(run_worker, std::cref(graph),
                                     std::cref(work_items), begin_index, end_index,
                                     std::ref(worker_results[worker_index]));
            }
        }

        for (std::thread& thread : threads) {
            thread.join();
        }

        write_output_file(paths.output_path, worker_results);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
