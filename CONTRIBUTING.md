# 📘 Guppy: Contibutor Guide

## 👋 Welcome! Here's how to contribute.

Thank you for taking the time to make Guppy better! A few things to know:

* Please don't work on anything substantial without confirming with the maintainers first. You may end up spending a lot of effort on something that's already in progress somewhere else, or just isn't a direction we want to take Guppy, and we'd hate to waste your time. That said, if it's small enough that you don't mind risking it, feel free to send the PR immediately!

* Any [issues labeled "good first issue"](https://github.com/storacha/guppy/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22) are definitely desired, so feel free to grab them! But please leave a comment saying you're working on it so no one else picks it up at the same time.

* Any *other* issues aren't necessarily something we want to do right now, or even at all. These issues are prioritized in the [Storacha team project](https://github.com/orgs/storacha/projects/1/), and may have some team context that's not obvious from the outside yet. Feel free to ask, though, if it's something you'd really like to work on!


## 💻 Installation & Setup

This section provides detailed instructions for setting up the Guppy client locally on Windows, macOS, and Linux.

### 🛠️ Prerequisites

* **Go** (v1.24.4 or higher)
* **Git**

### 🪟 Windows Setup (Important!)

> [!WARNING]
> **Critical:** Do **not** clone this repository into a folder synced by OneDrive (e.g., `Desktop`, `Documents`). OneDrive file locking often corrupts Go build artifacts, causing "Not a Valid Application" errors.

**Recommended Path:** `C:\guppy` or `C:\Users\YourName\Dev\guppy` (outside OneDrive).

1.  **Clone the Repository:**
    ```powershell
    git clone https://github.com/storacha/guppy.git
    cd guppy
    ```

2.  **Download Dependencies:**
    ```powershell
    go mod download
    ```

3.  **Build the Executable:**
    ```powershell
    # Build from the root directory
    go build -o guppy.exe .
    ```

4.  **Verify Installation:**
    ```powershell
    .\guppy.exe --help
    ```

### 🍎 macOS & 🐧 Linux Setup

1.  **Clone & Build:**
    ```bash
    git clone https://github.com/storacha/guppy.git
    cd guppy
    go mod download
    make build
    ```

2.  **Run:**
    ```bash
    ./guppy --help
    ```

3.  **Test:**
    ```bash
    make test
    ```

