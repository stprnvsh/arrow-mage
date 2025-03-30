#pragma once
#include <string>
#include <functional>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace crosslink {

enum class NotificationType {
    DATA_UPDATED,
    DATA_DELETED,
    METADATA_CHANGED
};

struct NotificationEvent {
    std::string dataset_id;
    NotificationType type;
    std::string source_language;
    std::string timestamp;
};

class NotificationSystem {
public:
    using CallbackFn = std::function<void(const NotificationEvent&)>;
    
    static NotificationSystem& instance();
    
    // Register for notifications
    std::string register_callback(const CallbackFn& callback);
    
    // Unregister a callback
    void unregister_callback(const std::string& callback_id);
    
    // Send notification to all listeners
    void notify(const NotificationEvent& event);
    
private:
    NotificationSystem();
    ~NotificationSystem();
    
    std::mutex mutex_;
    std::unordered_map<std::string, CallbackFn> callbacks_;
    
    // Generate a unique ID for callback registration
    std::string generate_callback_id();
};

} // namespace crosslink 