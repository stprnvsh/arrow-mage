#include "notification_system.h"
#include "arrow_bridge.h"
#include <random>
#include <sstream>
#include <iostream>

namespace crosslink {

NotificationSystem& NotificationSystem::instance() {
    static NotificationSystem instance;
    return instance;
}

NotificationSystem::NotificationSystem() {
    // Initialization logic if needed
}

NotificationSystem::~NotificationSystem() {
    // Cleanup logic if needed
}

std::string NotificationSystem::generate_callback_id() {
    // Use UUID generation logic similar to ArrowBridge
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    
    std::stringstream ss;
    ss << "callback-";
    for (int i = 0; i < 16; i++) {
        ss << std::hex << dis(gen);
    }
    
    return ss.str();
}

std::string NotificationSystem::register_callback(const CallbackFn& callback) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    std::string id = generate_callback_id();
    callbacks_[id] = callback;
    
    return id;
}

void NotificationSystem::unregister_callback(const std::string& callback_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    auto it = callbacks_.find(callback_id);
    if (it != callbacks_.end()) {
        callbacks_.erase(it);
    }
}

void NotificationSystem::notify(const NotificationEvent& event) {
    std::unordered_map<std::string, CallbackFn> callbacks_copy;
    
    {
        std::unique_lock<std::mutex> lock(mutex_);
        callbacks_copy = callbacks_;
    }
    
    // Call the callbacks without holding the lock
    for (const auto& [id, callback] : callbacks_copy) {
        try {
            callback(event);
        } catch (const std::exception& e) {
            // Log error but continue with other callbacks
            // In a real implementation, we'd want to use a proper logging system
            std::cerr << "Error in notification callback: " << e.what() << std::endl;
        }
    }
}

} // namespace crosslink 