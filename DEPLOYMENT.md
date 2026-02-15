# GitHub Actions Deployment Setup Guide

This guide explains how to set up automated deployment to your Oracle Cloud instance using GitHub Actions.

## Overview

The deployment workflow will:
- Sync code from GitHub to your Oracle Cloud instance (193.123.94.116)
- Install dependencies only when `requirements.txt` changes
- Preserve production data files, database, and server-specific configuration
- Deploy automatically on push to main, PR merges, releases, or manual trigger

## Prerequisites

- GitHub repository access
- SSH access to Oracle Cloud instance (ubuntu@193.123.94.116)
- Python virtual environment exists at `/home/ubuntu/plombery/venv`

## Step 1: Setup SSH Key for GitHub Actions

### Option A: Generate New SSH Key Pair (Recommended)

Generate a new SSH key pair specifically for GitHub Actions:

```bash
ssh-keygen -t ed25519 -C "github-actions" -f ~/.ssh/github_actions_key -N ""
```

This will create:
- Private key: `~/.ssh/github_actions_key`
- Public key: `~/.ssh/github_actions_key.pub`

### Option B: Use Existing SSH Key

If you already have an SSH key pair that you use to connect to the server (e.g., `id_rsa`), you can use that instead.

**WARNING:** Never add your personal SSH key to GitHub Secrets. Always create a dedicated key for GitHub Actions.

## Step 2: Add Public Key to Oracle Cloud Instance

Copy the public key to the server and add it to authorized_keys:

```bash
cat ~/.ssh/github_actions_key.pub | ssh ubuntu@193.123.94.116 "mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
```

Verify the key was added:

```bash
ssh ubuntu@193.123.94.116 "cat ~/.ssh/authorized_keys"
```

Test SSH connection with the new key:

```bash
ssh -i ~/.ssh/github_actions_key ubuntu@193.123.94.116 "echo 'Connection successful'"
```

## Step 3: Configure GitHub Secrets

Navigate to your GitHub repository:
1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Add each secret with the values below:

### Required Secrets

| Secret Name | Value |
|-------------|-------|
| `SSH_HOST` | `193.123.94.116` |
| `SSH_USER` | `ubuntu` |
| `SSH_PORT` | `22` |
| `REMOTE_PATH` | `/home/ubuntu/plombery` |
| `SSH_PRIVATE_KEY` | *(contents of private key file)* |

### How to add SSH_PRIVATE_KEY:

1. Copy the private key contents:
   ```bash
   cat ~/.ssh/github_actions_key
   ```

2. **Important:** Copy the entire key including:
   - The `-----BEGIN OPENSSH PRIVATE KEY-----` line
   - All the content in between
   - The `-----END OPENSSH PRIVATE KEY-----` line

3. Paste it as the value for `SSH_PRIVATE_KEY`

**Example:**
```
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
...
your-key-content-here
...
-----END OPENSSH PRIVATE KEY-----
```

## Step 4: Verify Server Configuration

### Check Directory Structure

SSH into the server and verify:

```bash
ssh ubuntu@193.123.94.116
cd /home/ubuntu/plombery
ls -la
```

Expected structure:
```
/home/ubuntu/plombery/
├── venv/                    # Virtual environment
├── src/                     # Source code
├── saved_data/              # Cached data (preserved)
├── saved_pages/             # Cached pages (preserved)
├── csv_downloads/           # Downloaded data (preserved)
├── *.json                   # Generated files (preserved)
├── plombery.db              # Database (preserved)
└── requirements.txt         # Dependencies
```

### Check Virtual Environment

```bash
cd /home/ubuntu/plombery
source venv/bin/activate
python3 --version
pip --version
plombery --version
```

### Check Config File

Ensure the config file exists and contains your secrets:

```bash
ls -la src/plombery/config/
cat src/plombery/config/config.ini
```

**Note:** The config file should be excluded from rsync to preserve server-specific settings.

## Step 5: Test the Deployment

### Manual Trigger

1. Go to **Actions** tab in your GitHub repository
2. Select **Deploy to Oracle Cloud** workflow
3. Click **Run workflow** → Select branch → Click **Run workflow**
4. Monitor the execution logs

### Verify Plombery Service

After deployment, verify the Plombery service is running:

```bash
ssh ubuntu@193.123.94.116 "systemctl status plomberly.service"
```

Expected output:
```
● plomberly.service - Plomberly Service
     Loaded: loaded (/etc/systemd/system/plomberly.service; enabled)
     Active: active (running) since Sun 2024-02-15 10:35:49 UTC
   Main PID: 1009 (python)
      Tasks: 8 (limit: 1060)
     Memory: 247.9M
        CPU: 4.390s
     CGroup: /system.slice/plomberly.service
             └─1009 /home/ubuntu/plombery/venv/bin/python /home/ubuntu/plombery/src/app.py
```

Key metrics to monitor:
- **Active**: Should be `active (running)`
- **Memory**: Should be stable (approx 200-300MB)
- **CPU**: Should increase during pipeline runs, then return to baseline
- **Tasks**: Number of running processes

Check service logs:
```bash
ssh ubuntu@193.123.94.116 "journalctl -u plomberly.service -f"
```

Restart the service if needed:
```bash
ssh ubuntu@193.123.94.116 "sudo systemctl restart plomberly.service"
```

### Common Service Issues

#### Elasticsearch Security Warning

You may see this warning in the service logs:

```
ElasticsearchWarning: Elasticsearch built-in security features are not enabled.
Without authentication, your cluster could be accessible to anyone.
```

This is expected if you're running Elasticsearch locally or without security configured. It's safe to ignore for development environments. For production, consider:
- Enabling Elasticsearch security (x-pack security)
- Using a reverse proxy with authentication
- Ensuring firewall rules restrict access

#### High Memory Usage

If memory exceeds 500MB consistently:
```bash
# Check memory usage
ssh ubuntu@193.123.94.116 "free -h"

# Check which processes are using memory
ssh ubuntu@193.123.94.116 "ps aux --sort=-%mem | head -10"

# If needed, restart the service to free memory
ssh ubuntu@193.123.94.116 "sudo systemctl restart plomberly.service"
```

#### Service Not Starting

If the service fails to start:
```bash
# Check detailed error logs
ssh ubuntu@193.123.94.116 "journalctl -xe -u plomberly.service"

# Common issues:
# 1. Virtual environment not found - verify path
# 2. Python version mismatch - check venv
# 3. Config file missing - create src/plombery/config/config.ini
```

### Automatic Trigger

Push to the main branch:

```bash
git add .
git commit -m "Test deployment"
git push origin main
```

## What Gets Deployed

### Files That ARE Deployed:
- `src/` - All source code (except config.ini)
- `requirements.txt` - Python dependencies

### Files That ARE NOT Deployed (Preserved on Server):
- `.git/`, `.github/` - Git metadata
- `tests/` - Test files
- `data/`, `saved_data/`, `saved_pages/` - Cached data
- `csv_downloads/` - Downloaded data
- `*.json` - Generated JSON files
- `plombery.db` - Production database
- `*.pyc`, `__pycache__/` - Python bytecode
- `venv/` - Virtual environment
- `src/plombery/config/config.ini` - Server-specific config

## Deployment Triggers

The workflow runs on:
- **Push to main/master** - Automatic deployment
- **Pull request merged to main/master** - Automatic deployment
- **New release published** - Automatic deployment
- **Manual workflow dispatch** - On-demand deployment

## Troubleshooting

### SSH Connection Failed

```bash
# Test SSH connection manually
ssh -i ~/.ssh/github_actions_key ubuntu@193.123.94.116

# Check server logs
ssh ubuntu@193.123.94.116 "journalctl -u sshd -n 50"
```

### Virtual Environment Not Found

The workflow expects the venv at `/home/ubuntu/plombery/venv`. If it's elsewhere, update the workflow.

```bash
# Find venv location
ssh ubuntu@193.123.94.116 "find /home/ubuntu -type d -name venv"
```

### Permission Denied

Ensure the SSH key has correct permissions:

```bash
chmod 600 ~/.ssh/github_actions_key
```

On the server:

```bash
ssh ubuntu@193.123.94.116 "chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys"
```

### Config File Overwritten

The workflow excludes `src/plombery/config/config.ini` from rsync. If it's being overwritten, verify the path matches exactly:

```bash
ssh ubuntu@193.123.94.116 "ls -la /home/ubuntu/plombery/src/plombery/config/"
```

### Dependencies Not Installing

Check if the venv is correctly set up:

```bash
ssh ubuntu@193.123.94.116 "cd /home/ubuntu/plombery && source venv/bin/activate && pip list"
```

## Security Best Practices

1. **Never commit SSH keys to the repository**
2. **Use a dedicated SSH key for GitHub Actions** (not your personal key)
3. **Rotate SSH keys regularly**
4. **Enable branch protection rules** to require review before pushing to main
5. **Consider requiring approval for deployments** in GitHub Actions settings

## Advanced Configuration

### Require Deployment Approval

To require manual approval before deploying:

1. Go to **Settings** → **Environments**
2. Create a new environment named `production`
3. Add required reviewers
4. Update the workflow:

```yaml
jobs:
  deploy:
    environment: production
    runs-on: ubuntu-latest
    ...
```

### Add Deployment Notifications

Add Slack notifications after the deployment summary step:

```yaml
- name: Notify Slack
  uses: 8398a7/action-slack@v3
  if: always()
  with:
    status: ${{ job.status }}
    text: 'Deployment to Oracle Cloud: ${{ job.status }}'
    webhook_url: ${{ secrets.SLACK_WEBHOOK_URL }}
```

## Monitoring Deployments

View deployment history and logs:
1. Go to **Actions** tab
2. Click on **Deploy to Oracle Cloud** workflow
3. View individual run details and logs

## Rollback Procedure

If a deployment causes issues:

1. **Immediate rollback** - SSH into server and manually revert:
   ```bash
   ssh ubuntu@193.123.94.116
   cd /home/ubuntu/plombery
   git log --oneline
   git checkout <previous-commit-hash>
   ```

2. **Or revert the commit** on GitHub:
   ```bash
   git revert <commit-hash>
   git push origin main
   ```

3. **The workflow will automatically deploy** the rollback.

## Support

For issues or questions:
- Check GitHub Actions logs in the repository
- Verify SSH connection manually
- Review server logs: `ssh ubuntu@193.123.94.116 "journalctl -xe"`
