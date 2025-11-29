# 📘 Guppy: Local Development & Usage Guide

This guide provides detailed instructions for setting up the Guppy client locally on Windows, macOS, and Linux. It covers environment configuration and the "Pre-Authorized" workflow for testing.

## 🛠️ Prerequisites

* **Go** (v1.24.4 or higher)
* **Node.js & NPM** (Required for key generation)
* **Git**

---

## 💻 Installation & Setup

### 🪟 Windows Setup (Important!)
**⚠️ Critical Warning:** Do **not** clone this repository into a folder synced by OneDrive (e.g., `Desktop`, `Documents`). OneDrive file locking often corrupts Go build artifacts, causing "Not a Valid Application" errors.

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
    go build -o guppy .
    ```

2.  **Run:**
    ```bash
    ./guppy --help
    ```

---

## 🔑 The "Hybrid" Auth Workflow

While interactive login is in development, use the **Storacha JS CLI** to generate your identity (DID) and proofs, then feed them into Guppy.

### Step 1: Generate Identity & Proofs (via JS CLI)

1.  **Install the Helper CLI:**
    ```bash
    npm install -g @storacha/cli
    ```

2.  **Generate Keys:**
    ```bash
    storacha key create
    ```
    *Save the output! You need the **DID** (`did:key:...`) and **Private Key** (`Mg...`).*

3.  **Create a Space:**
    ```bash
    storacha login
    storacha space create guppy-test-space
    ```
    *Save the **Space DID** returned (e.g., `did:key:z6Mk...`).*

4.  **Create a Proof File:**
    Authorize your local key to upload to that space:
    ```bash
    # Replace <YOUR_DID> with the DID from Step 2
    storacha delegation create -c 'store/*' -c 'upload/*' <YOUR_DID> -o proof.ucan
    ```

### Step 2: Configure Guppy Environment

Set your private key so Guppy knows who you are.

**Windows (PowerShell):**
```powershell
$env:GUPPY_PRIVATE_KEY="Mg...<YOUR_PRIVATE_KEY>"

**Mac/Linux**
```bash
export GUPPY_PRIVATE_KEY="Mg...<YOUR_PRIVATE_KEY>"
```

Step 3: Perform Upload
1. Register the Source:

# Usage: guppy upload sources add <SPACE_DID> <FILE_PATH>
.\guppy.exe upload sources add did:key:z6Mk... C:\path\to\file.txt

2. Execute Upload
.\guppy.exe upload did:key:z6Mk... --proof .\proof.ucan

❓ Troubleshooting
"The term 'go' is not recognized": Ensure C:\Program Files\Go\bin is in your System PATH.

"Program failed to run... not a valid application": You are likely building inside a OneDrive folder. Move the project to C:\ and run go clean -cache then rebuild.

"Path not specified" (during login): If guppy login fails, manually create the config directory: mkdir ~/.storacha.

"Unauthorized": Ensure you are passing a valid .ucan proof file generated via the JS CLI.

