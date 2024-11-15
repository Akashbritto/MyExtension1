
document.getElementById("clickbutton").addEventListener("click", function() {
    chrome.tabs.create({ url: "https://www.example.com" });
});