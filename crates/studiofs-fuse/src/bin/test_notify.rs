use std::sync::Arc;
use tokio::sync::Notify;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let notify = Arc::new(Notify::new());
    
    let notify_clone = notify.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("Calling notify_waiters");
        notify_clone.notify_waiters();
    });
    
    // Wait for the other task to call notify_waiters
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create future AFTER notify_waiters
    let notified = notify.notified(); 
    
    println!("Polling notified()");
    // Now we poll it with a timeout to see if it hangs
    let res = tokio::time::timeout(Duration::from_millis(100), notified).await;
    match res {
        Ok(_) => println!("It resolved!"),
        Err(_) => println!("It timed out!"),
    }
}
