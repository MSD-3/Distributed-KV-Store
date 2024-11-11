#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <vector>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <functional>
#include <iomanip>

#define NUM_NODES 10
#define DATA "organizations.csv"

using namespace std;

template<typename K, typename V>
class LRUCache {
private:
    int capacity;
    mutable mutex cache_mutex;
    mutable list<pair<K, V>> items;
    mutable unordered_map<K, typename list<pair<K, V>>::iterator> cache;

    //cache statistics tracker
    mutable uint64_t cache_accesses{0};
    mutable uint64_t cache_hits{0};
    mutable uint64_t cache_misses{0};

    //helper function to update LRU order
    void touch(typename unordered_map<K, typename list<pair<K, V>>::iterator>::iterator cache_it) const {
        //move the accessed item to front of the list
        auto list_it = cache_it->second;
        auto value = move(*list_it);
        items.erase(list_it);
        items.push_front(move(value));
        cache_it->second = items.begin();
    }

public:
    explicit LRUCache(int size) : capacity(size) {}
    //put function for cache
    void put(const K& key, const V& value) {
        lock_guard<mutex> lock(cache_mutex);
        auto it = cache.find(key);
        
        if (it != cache.end()) {
            //update existing item
            it->second->second = value;
            touch(it);
        } else {
            // Add new item
            if (cache.size() >= capacity) {
                //remove least recently used item
                cache.erase(items.back().first);
                items.pop_back();
            }
            items.push_front({key, value});
            cache[key] = items.begin();
        }
    }
    //get function for cache
     pair<bool, V> get(const K& key) const {
        lock_guard<mutex> lock(cache_mutex);
        cache_accesses++;
        
        auto it = cache.find(key);
        if (it == cache.end()) {
            cache_misses++;
            return {false, V()};
        }
        cache_hits++;
        auto value = it->second->second;
        touch(it);
        return {true, value};
    }
    //checks if key is present in cache
    bool contains(const K& key) const {
        lock_guard<mutex> lock(cache_mutex);
        return cache.find(key) != cache.end();
    }
    //returns cache size
    int size() const {
        lock_guard<mutex> lock(cache_mutex);
        return cache.size();
    }
    //clears cache
    void clear() {
        lock_guard<mutex> lock(cache_mutex);
        items.clear();
        cache.clear();
    }

    //get cache contents    
     vector<tuple<K, V, int >> getCacheContents() const {
        lock_guard<mutex> lock(cache_mutex);
        vector<tuple<K, V, int >> contents;
        int  lru_position = 0;
        
        for (const auto& item : items) {
            contents.emplace_back(item.first, item.second, lru_position++);
        }
        return contents;
    }

    //get cache accesses
     uint64_t getAccesses() const {
        lock_guard<mutex> lock(cache_mutex);
        return cache_accesses;
    }
    //get cache hits
    uint64_t getHits() const {
        lock_guard<mutex> lock(cache_mutex);
        return cache_hits;
    }
    //get cache misses
    uint64_t getMisses() const {
        lock_guard<mutex> lock(cache_mutex);
        return cache_misses;
    }
    //get cache MPKI
    double getMPKI() const {
        lock_guard<mutex> lock(cache_mutex);
        if (cache_accesses == 0) return 0.0;
        return (cache_misses * 1000.0) / cache_accesses;
    }
    
};
class Node {
private:
    LRUCache<string, string> cache;
    unordered_map<string, string> storage;
    mutable mutex storageMutex;
    int port;
    vector<pair<string, int>> peers;
    bool isRunning;
    thread listenerThread;
    unordered_map<string, int> updateVersions;
    int currentVersion;
    
    // Sharding-related members
    int nodeId;
    int totalNodes;
    hash<string> hasher;

public:
    //constructor
    Node(int listenPort, int cacheSize, int id, int total) : cache(cacheSize), port(listenPort), isRunning(true), currentVersion(0),nodeId(id), totalNodes(total) {
        listenerThread = thread(&Node::startListener, this);
        this_thread::sleep_for(chrono::milliseconds(100));
    }
    //destructor
    ~Node() {
        isRunning = false;
        if (listenerThread.joinable()) {
            listenerThread.join();
        }
    }

    //returns storage size
    int getStorageSize() const {
        lock_guard<mutex> lock(storageMutex);
        return storage.size();
    }

    //check if the node is responsible for the key
    bool isResponsibleFor(const string& key) const {
        return (hasher(key) % totalNodes) == nodeId;    //using a hashmap to map data to nodes
    }

    //node loads from CSV
    void loadFromCSV(const string& filename) {
        ifstream file(filename);
        string line;
        
        while (getline(file, line)) {
            istringstream iss(line);
            string key, value;
            
            if (getline(iss, key, ',') && getline(iss, value)) {
                //only store if this node is responsible for the key
                if (isResponsibleFor(key)) {
                    lock_guard<mutex> lock(storageMutex);
                    storage[key] = value;
                    updateVersions[key] = currentVersion;
                }
            }
        }
    }

    //adds peer to the peer lista
    void addPeer(const string& ip, int peerPort) {
        peers.push_back({ip, peerPort});
        //cout << "Added peer: " << ip << ":" << peerPort << endl;
    }

    //prints peer information
    void printPeers() {
        cout << "Node " << nodeId << " peers:" << endl;
        for ( auto& peer : peers) {
            cout << "  " << peer.first << ":" << peer.second << endl;
        }
    }

    //GET function
    string get(string& key) {
        //cout << "Debug: GET request for key: " << key << " on node " << nodeId << endl;
        
        // Check cache first
        auto [found, cachedValue] = cache.get(key);
        if (found && !cachedValue.empty()) {
            //cout << "Debug: Found in cache: " << cachedValue << endl;
            return cachedValue;
        }
        
        //redirect if not resposnible
        if (!isResponsibleFor(key)) {
            //cout << "Debug: Not responsible for key, redirecting" << endl;
            string redirectedValue = redirectGet(key);
            if (!redirectedValue.empty()) {
                //cache the value received from the responsible node
                cache.put(key, redirectedValue);
            }
            return redirectedValue;
        }
        //check for data
        lock_guard<mutex> lock(storageMutex);
        auto it = storage.find(key);
        if (it != storage.end() && !it->second.empty()) {
            //cout << "Debug: Found in storage: " << it->second << endl;
            // Cache the value we're about to return
            cache.put(key, it->second);
            return it->second;
        }

        //cout << "Debug: Key not found in node " << nodeId << endl;
        return "";
    }


    //SET function
bool set(const string& key, const string& value) {
    //cout << "Debug: SET request for key: " << key << " value: " << value << " on node " << nodeId << endl;
    //cout << "Debug: Hash of key: " << hasher(key) << endl;
    //cout << "Debug: Total nodes: " << totalNodes << endl;
    //cout << "Debug: Responsible node: " << (hasher(key) % totalNodes) << endl;

    if (!isResponsibleFor(key)) {
        //cout << "Debug: Not responsible for key, redirecting to correct node" << endl;
        return redirectSet(key, value);
    }

    try {
        lock_guard<mutex> lock(storageMutex);
        storage[key] = value;
        cache.put(key, value);
        updateVersions[key] = ++currentVersion;
        
        //cout << "Debug: Successfully stored value locally" << endl;
        return true;
    } catch (const exception& e) {
        cerr << "Debug: Error storing value: " << e.what() << endl;
        return false;
    }
}
    
    pair<int , int > getKeyRange() const {
            int rangeSize = numeric_limits<int >::max() / totalNodes;
            int start = nodeId * rangeSize;
            int end = (nodeId == totalNodes - 1) ? numeric_limits<int >::max() : start + rangeSize - 1;
            return {start, end};
        }

        //get all data 
        vector<string> getStoredKeys() const {
            vector<string> keys;
            lock_guard<mutex> lock(storageMutex);
            for (const auto& pair : storage) {
                keys.push_back(pair.first);
            }
            return keys;
        }

    int getCacheSize() const {
        return cache.size();
    }

    void printCacheStats() const {
    cout << "\n=== Cache Statistics for Node " << nodeId << " ===\n";
    cout << "Cache Size: " << cache.size() << "/" << getCacheSize() << "\n";
    cout << "Total Accesses: " << cache.getAccesses() << "\n";
    cout << "Cache hits: " << cache.getHits() << "\n";
    cout << "Cache Misses: " << cache.getMisses() << "\n";
    cout << "Cache MPKI (Misses Per Kilo Instructions): " << fixed << setprecision(2) << cache.getMPKI() << "\n\n";
    
    cout << "Cache Contents (LRU order - 0 is most recently used):\n";
    cout << setw(20) << "Key"<< " | " << "LRU Position\n";
    cout << string(60, '-') << "\n";
    
    auto contents = cache.getCacheContents();
    for (const auto& [key, value, lru_pos] : contents) {
        cout << setw(20) << key << " | " 
                  << lru_pos << "\n";
    }
    cout << string(60, '-') << "\n";
}

    //shows all the storage contents of the node
    void dumpStorage() const {
        lock_guard<mutex> lock(storageMutex);
        cout << "\nStorage contents for Node " << nodeId << ":\n";
        for (const auto& [key, value] : storage) {
            cout << key << " => " << value << "\n";
        }
        cout << endl;
    }   
    
private:
    //redirects request to target node
bool redirectSet(const string& key, const string& value) {
    int targetNode = hasher(key) % totalNodes;
    //cout << "Debug: Redirecting SET - Key: " << key << endl;
    //cout << "Debug: Target node: " << targetNode << endl;
    //cout << "Debug: Looking for port: " << (8081 + targetNode) << endl;
    
    for (const auto& peer : peers) {
        //cout << "Debug: Checking peer " << peer.first << ":" << peer.second << endl;
        if (peer.second == (8081 + targetNode)) {
            //cout << "Debug: Found matching peer, attempting to propagate update" << endl;
            bool result = propagateUpdate(peer.first, peer.second, key, value, currentVersion);
            //cout << "Debug: Propagate update result: " << (result ? "success" : "failure") << endl;
            return result;
        }
    }
    
    cerr << "Debug: No peer found for node " << targetNode << endl;
    return false;
}
    string redirectGet(const string& key) {
        int targetNode = hasher(key) % totalNodes;
        //cout << "Debug: Node " << nodeId << " redirecting GET - Key: " << key << ", Target Node: " << targetNode << endl;
        
        for (const auto& peer : peers) {
            if (peer.second == (8081 + targetNode)) {
                int sock = socket(AF_INET, SOCK_STREAM, 0);
                if (sock < 0) {
                    cerr << "Failed to create socket for redirect" << endl;
                    return "";
                }

                struct timeval tv;
                tv.tv_sec = 1;
                tv.tv_usec = 0;
                setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

                sockaddr_in peerAddr{};
                peerAddr.sin_family = AF_INET;
                peerAddr.sin_port = htons(peer.second);
                if (inet_pton(AF_INET, peer.first.c_str(), &peerAddr.sin_addr) <= 0) {
                    cerr << "Invalid peer IP address" << endl;
                    close(sock);
                    return "";
                }

                if (connect(sock, (struct sockaddr*)&peerAddr, sizeof(peerAddr)) < 0) {
                    cerr << "Failed to connect to peer for redirect" << endl;
                    close(sock);
                    return "";
                }

                // Send a GET request to the responsible node
                string request = "GET:" + key;
                send(sock, request.c_str(), request.length(), 0);

                char buffer[4096] = {0};
                int bytesRead = read(sock, buffer, sizeof(buffer) - 1);
                close(sock);

                if (bytesRead > 0) {
                    string response(buffer);
                    //cout << "Debug: Node " << nodeId << " received response from peer: '" << response << "'" << endl;
                    return response;
                }
            }
        }
        
        cerr << "No peer found for node " << targetNode << endl;
        return "";
    }

    void handleIncomingRequest(int clientSocket) {
        char buffer[4096] = {0};
        int bytesRead = read(clientSocket, buffer, sizeof(buffer) - 1);
        if (bytesRead <= 0) {
            cerr << "Error reading from socket" << endl;
            close(clientSocket);
            return;
        }

        string request(buffer);
        istringstream iss(request);
        string cmd, key, value;
        getline(iss, cmd, ':');
        
        try {
            if (cmd == "GET") {
                getline(iss, key);
                key.erase(remove_if(key.begin(), key.end(), 
                        [](unsigned char c) { return isspace(c); }), key.end());
                
                string result = get(key);
                send(clientSocket, result.c_str(), result.length(), 0);
            }
            else if (cmd == "SET") {
                getline(iss, key, ':');
            getline(iss, value);
            
            //remove any whitespace
            key.erase(remove_if(key.begin(), key.end(), 
                     [](unsigned char c) { return isspace(c); }), key.end());
            
            //cout << "Debug: Node " << nodeId << " processing SET request for key: '" << key << "' value: '" << value << "'" << endl;
            
            bool success = set(key, value);
            
            //always send an acknowledgment
            const char* response = success ? "ACK" : "NAK";
            send(clientSocket, response, strlen(response), 0);
            
            //cout << "Debug: Node " << nodeId << " sent response: " << response << endl;
            }
        }
        catch (const exception& e) {
            cerr << "Error handling request on node " << nodeId << ": " 
                    << e.what() << endl;
            const char* error = "ERR";
            send(clientSocket, error, strlen(error), 0);
        }
    }

//request listener
void startListener() {
        int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (serverSocket < 0) {
            cerr << "Error creating socket\n";
            return;
        }

        int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in serverAddr{};
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_addr.s_addr = INADDR_ANY;
        serverAddr.sin_port = htons(port);

        if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            cerr << "Error binding socket on port " << port << "\n";
            close(serverSocket);
            return;
        }

        listen(serverSocket, 10);

        while (isRunning) {
            sockaddr_in clientAddr{};
            socklen_t clientLen = sizeof(clientAddr);
            
            int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientLen);
            if (clientSocket < 0) continue;

            thread([this, clientSocket]() {
                handleIncomingRequest(clientSocket);
                close(clientSocket);
            }).detach();
        }

        close(serverSocket);
    } 
    //handles reuqest and returns output
    void handleRequest(int clientSocket) {
        char buffer[1024] = {0};
       int  bytesRead = read(clientSocket, buffer, 1024);
        if (bytesRead <= 0) return;
        
        string request(buffer);
        istringstream iss(request);
        string cmd, key, value;
        
        getline(iss, cmd, ':');
        if (cmd == "GET") {
            getline(iss, key);
            string result = get(key);
            send(clientSocket, result.c_str(), result.length(), 0);
        } 
        else if (cmd == "SET") {
            getline(iss, key, ':');
            getline(iss, value);
            set(key, value);
            const char* ack = "ACK";
            send(clientSocket, ack, strlen(ack), 0);
        }
    }

     bool tryConnect(const string& ip, int port) const {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return false;

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        sockaddr_in peerAddr{};
        peerAddr.sin_family = AF_INET;
        peerAddr.sin_port = htons(port);
        if (inet_pton(AF_INET, ip.c_str(), &peerAddr.sin_addr) <= 0) {
            close(sock);
            return false;
        }

        bool success = connect(sock, (struct sockaddr*)&peerAddr, sizeof(peerAddr)) >= 0;
        close(sock);
        return success;
    }

//maintains consistency accross nodes
bool propagateUpdate(const string& peerIp, int peerPort, const string& key, const string& value,int version) {
    //cout << "Debug: Attempting to propagate update to " << peerIp << ":" << peerPort << endl;
    
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        cerr << "Debug: Failed to create socket: " << strerror(errno) << endl;
        return false;
    }

    struct timeval tv;  //propagate update interval
    tv.tv_sec = 5;  
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in peerAddr{};
    peerAddr.sin_family = AF_INET;
    peerAddr.sin_port = htons(peerPort);
    
    if (inet_pton(AF_INET, peerIp.c_str(), &peerAddr.sin_addr) <= 0) {
        cerr << "Debug: Invalid peer IP address: " << strerror(errno) << endl;
        close(sock);
        return false;
    }

    if (connect(sock, (struct sockaddr*)&peerAddr, sizeof(peerAddr)) < 0) {
        cerr << "Debug: Failed to connect to peer: " << strerror(errno) << endl;
        close(sock);
        return false;
    }

    //cout << "Debug: Connected to peer successfully" << endl;

    try {
        string request = "SET:" + key + ":" + value + ":" + to_string(version);
        //cout << "Debug: Sending request: " << request << endl;
        
        int sent = send(sock, request.c_str(), request.length(), 0);
        if (sent < 0) {
            cerr << "Debug: Failed to send data: " << strerror(errno) << endl;
            close(sock);
            return false;
        }
        //cout << "Debug: Sent " << sent << " bytes" << endl;

        // Wait for response with timeout
        char response[4] = {0};
        int bytesRead = recv(sock, response, sizeof(response) - 1, 0);
        //cout << "Debug: Received " << bytesRead << " bytes as response: '" << (bytesRead > 0 ? response : "nothing") << "'" << endl;
        
        close(sock);
        
        if (bytesRead <= 0) {
            cerr << "Debug: No response received from peer" << endl;
            return false;
        }
        
        return (strcmp(response, "ACK") == 0);
    }
    catch (const exception& e) {
        cerr << "Debug: Error in propagateUpdate: " << e.what() << endl;
        close(sock);
        return false;
    }
}
};


class KVStoreInterface {
private:
    vector<unique_ptr<Node>> nodes;
    int totalNodes;
    int currentNode;

    void printHelp() {
        cout << "\n=== Distributed Key-Value Store Commands ===\n"
                  << "GET <key>           - Retrieve value for key\n"
                  << "PUT <key> <value>   - Store key-value pair\n"
                  << "DELETE <key>        - Delete key-value pair\n"
                  << "STATUS              - Show cluster status\n"
                  << "CACHE <node_id>     - Show cache statistics for specific node\n"
                  << "DUMP                - Show all stored data\n"
                  << "USE <node_id>       - Switch to specific node for sending requests\n"
                  << "NODE                - Show currently selected node\n"
                  << "HELP                - Show this help message\n"
                  << "EXIT                - Exit the program\n"
                  << "==============================================\n";
    }

    void showStatus() {
        cout << "\n=== Cluster Status ===\n";
        for (int i = 0; i < nodes.size(); i++) {
            auto range = nodes[i]->getKeyRange();
            auto keys = nodes[i]->getStoredKeys();
            
            cout << "Node " << i << " (Port " << (8081 + i) << "):\n"
                     << "  Storage Size: " << nodes[i]->getStorageSize() << "\n"
                     << "  Cache Size: " << nodes[i]->getCacheSize() << "\n"
                     << "  Hash Range: " << hex << range.first 
                     << " - " << range.second << dec << "\n"
                     << "  Stored Keys: ";
            
            int keyCount = 0;
            for (const auto& key : keys) {
                if (keyCount++ >= 5) {
                    cout << "...";
                    break;
                }
                cout << key << " ";
            }
            cout << "\n";
        }
        cout << "====================\n";
    }

public:
    KVStoreInterface(int numNodes = 10) : totalNodes(numNodes), currentNode(0) {
        initializeCluster();
    }
    
    //function to initialse cluster
     void initializeCluster() {
        cout << "Initializing cluster with " << totalNodes << " nodes...\n";

        //create nodes
        for (int i = 0; i < totalNodes; i++) {
            nodes.push_back(make_unique<Node>(8081 + i, 10, i, totalNodes));
        }

        //add peers to each node
        for (int i = 0; i < totalNodes; i++) {
            for (int j = 0; j < totalNodes; j++) {
                if (i != j) {
                    //cout << "Node " << i << " adding peer node " << j << endl;
                    nodes[i]->addPeer("127.0.0.1", 8081 + j);
                }
            }
        }

        // Print peer configuration for verification
        // for (int i = 0; i < totalNodes; i++) {
        //     nodes[i]->printPeers();
        // }

        //load initial data
        for (auto& node : nodes) {
            node->loadFromCSV(DATA);
        }

        //wait for nodes to initialize
        this_thread::sleep_for(chrono::milliseconds(500));
        cout << "Cluster initialized successfully!\n";
    }

   void startInterface() {
        string line;
        printHelp();

        while (true) {
            cout << "\nNode[" << currentNode << "]> ";
            getline(cin, line);
            
            if (line.empty()) continue;

            istringstream iss(line);
            string command;
            iss >> command;

            for (char& c : command) {
                c = toupper(c);
            }

            try {
                if (command == "USE") {
                    int nodeId;
                    if (!(iss >> nodeId) || nodeId < 0 || nodeId >= totalNodes) {
                        cout << "Error: Invalid node ID. Must be between 0 and " 
                                << (totalNodes - 1) << "\n";
                        continue;
                    }
                    currentNode = nodeId;
                    cout << "Switched to Node " << currentNode << "\n";
                }
                else if (command == "NODE") {
                    cout << "Currently using Node " << currentNode 
                            << " (Port " << (8081 + currentNode) << ")\n";
                }
                else if (command == "DUMP") {
                    for (int i = 0; i < nodes.size(); i++) {
                        nodes[i]->dumpStorage();
                    }
                }
                else if (command == "GET") {
                    string key;
                    if (!(iss >> key)) {
                        cout << "Error: GET requires a key\n";
                        continue;
                    }

                    auto start = chrono::high_resolution_clock::now();
                    string value = nodes[currentNode]->get(key); 
                    auto end = chrono::high_resolution_clock::now();
                    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);

                    if (!value.empty()) {
                        cout << "Value: " << value << "\n";
                    } else {
                        cout << "Key not found\n";
                    }
                    cout << "Operation took " << duration.count() << " microseconds\n";
                }
                else if (command == "PUT") {
                    string key, value;
                    if (!(iss >> key)) {
                        cout << "Error: PUT requires a key and value\n";
                        continue;
                    }
                    
                    getline(iss >> ws, value);
                    if (value.empty()) {
                        cout << "Error: PUT requires a value\n";
                        continue;
                    }

                    auto start = chrono::high_resolution_clock::now();
                    bool success = nodes[currentNode]->set(key, value);
                    auto end = chrono::high_resolution_clock::now();
                    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);

                    if (success) {
                        cout << "Successfully stored key-value pair\n";
                    } else {
                        cout << "Failed to store key-value pair\n";
                    }
                    cout << "Operation took " << duration.count() << " microseconds\n";
                }
                else if (command == "DELETE") {
                    string key;
                    if (!(iss >> key)) {
                        cout << "Error: DELETE requires a key\n";
                        continue;
                    }

                    auto start = chrono::high_resolution_clock::now();
                    bool success = nodes[currentNode]->set(key, "");
                    auto end = chrono::high_resolution_clock::now();
                    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);

                    if (success) {
                        cout << "Successfully deleted key\n";
                    } else {
                        cout << "Failed to delete key\n";
                    }
                    cout << "Operation took " << duration.count() << " microseconds\n";
                }
                else if (command == "STATUS") {
                    showStatus();
                }
                else if (command == "CACHE") {
                    int nodeId;
                    if (!(iss >> nodeId) || nodeId < 0 || nodeId >= totalNodes) {
                        cout << "Error: CACHE requires a valid node ID (0-" 
                                << (totalNodes-1) << ")\n";
                        continue;
                    }
                    nodes[nodeId]->printCacheStats();
                }
                else if (command == "HELP") {
                    printHelp();
                }
                else if (command == "EXIT") {
                    cout << "Shutting down...\n";
                    exit(0);
                    break;
                }
                else {
                    cout << "Unknown command. Type 'HELP' for available commands.\n";
                }
            }
            catch (const exception& e) {
                cout << "Error: " << e.what() << "\n";
            }
        }
    }
     void showNodeInfo() {
        cout << "\n=== Node Information ===\n";
        for (int i = 0; i < totalNodes; i++) {
            cout << "Node " << i << " (Port " << (8081 + i) << "):\n";
            //nodes[i]->printPeers();
            cout << endl;
        }
        cout << "=====================\n";
    }
};

int main() {
    try {
        cout << "Starting cluster...\n";
        KVStoreInterface interface(NUM_NODES);
        cout << "Cluster started successfully.\n";
        interface.startInterface();
    }
    catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}