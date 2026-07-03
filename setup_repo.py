import os
import subprocess
import sys

def run_cmd(args, desc):
    print(f"Running: {' '.join(args)} ({desc})...")
    try:
        res = subprocess.run(args, capture_output=True, text=True, check=True)
        print(res.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        print(f"Stderr: {e.stderr}")
        sys.exit(1)

def main():
    print("=== Git Repository Automation Setup ===")
    
    # 1. Create .gitignore if it doesn't exist
    gitignore_path = ".gitignore"
    gitignore_content = """# Virtual environment
.venv/
ENV/
env/

# Python cache
__pycache__/
*.pyc
*.pyo
*.pyd
.ipynb_checkpoints

# Databases & Runtime persistence
*.db
*.db-journal
*.sqlite

# Testing cache
.pytest_cache/
.coverage
htmlcov/

# IDE files & Operating system
.idea/
.vscode/
*.swp
*.swo
.DS_Store
"""
    
    if not os.path.exists(gitignore_path):
        print(f"Creating {gitignore_path}...")
        with open(gitignore_path, "w") as f:
            f.write(gitignore_content)
    else:
        print(f"{gitignore_path} already exists. Skipping creation.")
        
    # 2. Check if git is installed
    try:
        subprocess.run(["git", "--version"], capture_output=True, check=True)
    except (subprocess.SubprocessError, FileNotFoundError):
        print("Error: Git is not installed on this system.")
        sys.exit(1)
        
    # 3. Initialize git repository
    if not os.path.exists(".git"):
        run_cmd(["git", "init"], "Initializing git repo")
        run_cmd(["git", "checkout", "-b", "main"], "Setting default branch to main")
    else:
        print("Git repository already initialized. Skipping 'git init'.")
        
    # 4. Stage and commit files
    run_cmd(["git", "add", "."], "Staging all files")
    
    # Check if there are changes to commit
    status_res = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if status_res.stdout.strip():
        run_cmd(["git", "commit", "-m", "feat: initial commit for industrial-grade event-driven agent swarm framework"], "Committing files")
        print("\nCommit successful!")
    else:
        print("No changes to commit. Everything is up-to-date.")
        
    print("\n" + "="*60)
    print("GIT REPOSITORY READY FOR GITHUB DEPLOYMENT!")
    print("="*60)
    print("To link this to your company's GitHub repository and push, run:")
    print("  git remote add origin <your-company-github-repo-url>")
    print("  git push -u origin main")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
