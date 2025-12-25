#!/bin/bash

set -e   # Exit immediately if any command fails

echo "======================================"
echo " E-Commerce Data Pipeline Setup Script "
echo "======================================"

# --------------------------------------
# Step 1: Detect Python
# --------------------------------------
echo "Checking Python installation..."

if command -v python3 >/dev/null 2>&1; then
    PYTHON=python3
elif command -v python >/dev/null 2>&1; then
    PYTHON=python
else
    echo "❌ Python not found."
    echo "Please install Python 3.8+ and add it to PATH."
    exit 1
fi

echo "✅ Using Python: $($PYTHON --version)"

# --------------------------------------
# Step 2: Create virtual environment
# --------------------------------------
echo "Creating virtual environment..."

if [ -d "venv" ]; then
    echo "⚠️  Virtual environment already exists. Skipping creation."
else
    $PYTHON -m venv venv
    echo "✅ Virtual environment created."
fi

# --------------------------------------
# Step 3: Activate virtual environment
# --------------------------------------
echo "Activating virtual environment..."

if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
elif [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
else
    echo "❌ Virtual environment activation script not found."
    exit 1
fi

echo "✅ Virtual environment activated."

# --------------------------------------
# Step 4: Upgrade pip (CORRECT WAY)
# --------------------------------------
echo "Upgrading pip inside virtual environment..."
python -m pip install --upgrade pip

# --------------------------------------
# Step 5: Install dependencies
# --------------------------------------
if [ ! -f "requirements.txt" ]; then
    echo "❌ requirements.txt not found."
    exit 1
fi

echo "Installing Python dependencies..."
python -m pip install -r requirements.txt

# --------------------------------------
# Done
# --------------------------------------
echo "======================================"
echo " 🎉 Environment setup completed successfully "
echo "======================================"
