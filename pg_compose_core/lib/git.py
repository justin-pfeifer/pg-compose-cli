
import tempfile
import subprocess
import os
import re

class GitRepoContext:
    """Context manager for git repository operations."""
    
    def __init__(self, repo_url: str, target_path: str = None):
        self.repo_url = repo_url
        self.target_path = target_path
        self.tmp_dir = None
        self.working_dir = None
        
    def __enter__(self):
        """Clone the repository and return the working directory path."""
        # Parse branch or commit from URL if specified
        ref = None
        repo_url = self.repo_url
        if "#" in repo_url:
            repo_url, ref = repo_url.split("#", 1)

        
        # Try different temp directory locations to avoid permission issues
        for temp_location in [None, os.path.expanduser("~/temp"), os.path.expanduser("~/Desktop/temp")]:
            try:
                if temp_location:
                    os.makedirs(temp_location, exist_ok=True)
                self.tmp_dir = tempfile.mkdtemp(dir=temp_location)
                break
            except (OSError, PermissionError):
                continue
        
        if self.tmp_dir is None:
            raise ValueError("Could not create temporary directory due to permission issues")
        
        # Determine if ref is a commit hash (40-character hex string) or branch
        is_commit = ref and re.match(r'^[a-fA-F0-9]{40}$', ref)
        
        # Clone the repository
        clone_cmd = ["git", "clone"]
        if is_commit:
            # For commits, we need full history, so don't use --depth 1
            if ref:
                clone_cmd.extend(["-b", "main"])  # Clone main branch first
        else:
            # For branches, we can use shallow clone
            clone_cmd.extend(["--depth", "1"])
            if ref:
                clone_cmd.extend(["-b", ref])
        
        clone_cmd.extend([repo_url, self.tmp_dir])
        try:
            subprocess.run(clone_cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            # If main branch doesn't exist, try master
            if is_commit and "main" in clone_cmd:
                clone_cmd[clone_cmd.index("-b") + 1] = "master"
                subprocess.run(clone_cmd, check=True, capture_output=True, text=True)
            else:
                error_msg = e.stderr
                if "unable to connect" in error_msg and repo_url.startswith("git://"):
                    error_msg += "\n\nNote: The git:// protocol may be blocked by your network or GitHub. Try using https:// or ssh:// instead."
                raise ValueError(f"Failed to clone repository: {error_msg}")
        except FileNotFoundError:
            raise ValueError("Git is not installed or not in PATH")
        
        # If we cloned for a specific commit, checkout that commit
        if is_commit:
            try:
                subprocess.run(["git", "checkout", ref], cwd=self.tmp_dir, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                raise ValueError(f"Failed to checkout commit {ref}: {e.stderr}")
        
        # Always return the repository root as working directory
        # The target_path will be used by the caller to construct the full path
        self.working_dir = self.tmp_dir
        
        # Check if the target_path exists in the repository
        if self.target_path:
            target_full_path = os.path.join(self.tmp_dir, self.target_path)
            if not os.path.exists(target_full_path):
                # If it's a directory path (doesn't end with .sql), it should exist
                if not self.target_path.endswith('.sql'):
                    raise ValueError(f"Directory path '{self.target_path}' does not exist in repository")
                # If it's a .sql file that doesn't exist, that's fine - it will be treated as new
        
        return self.working_dir
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - temp directory will be cleaned up by OS."""
        pass

def extract_from_git_repo(repo_url: str, target_path: str = None) -> GitRepoContext:
    """Extract schema objects from a git repository by cloning to temp directory.
    
    Args:
        repo_url: Git URL in format git://url or git@url, optionally with #branch or #commit suffix
        target_path: Optional path to specific directory or file within the repository
        
    Returns:
        GitRepoContext: A context manager that provides the working directory path
    """
    return GitRepoContext(repo_url, target_path)
