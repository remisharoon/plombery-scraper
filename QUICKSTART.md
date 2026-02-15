# Quick Start: GitHub Actions Deployment

## Quick Setup (5 minutes)

### 1. Generate SSH Key
```bash
ssh-keygen -t ed25519 -C "github-actions" -f ~/.ssh/github_actions_key -N ""
```

### 2. Add Public Key to Server
```bash
cat ~/.ssh/github_actions_key.pub | ssh ubuntu@193.123.94.116 "mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys"
```

### 3. Test SSH Connection
```bash
ssh -i ~/.ssh/github_actions_key ubuntu@193.123.94.116 "echo 'Success!'"
```

### 4. Add GitHub Secrets

Go to: **Settings → Secrets and variables → Actions → New repository secret**

Add these 5 secrets:

| Secret Name | Value |
|-------------|-------|
| `SSH_HOST` | `193.123.94.116` |
| `SSH_USER` | `ubuntu` |
| `SSH_PORT` | `22` |
| `REMOTE_PATH` | `/home/ubuntu/plombery` |
| `SSH_PRIVATE_KEY` | `cat ~/.ssh/github_actions_key` (copy entire contents) |

### 5. Test Deployment

Push to main to trigger deployment:
```bash
git add .
git commit -m "Test GitHub Actions deployment"
git push origin main
```

Or trigger manually:
1. Go to **Actions** tab
2. Select **Deploy to Oracle Cloud**
3. Click **Run workflow**

## What Happens During Deployment

1. ✅ SSH to server using the private key
2. ✅ Detect Python version on remote
3. ✅ Check if `requirements.txt` changed
4. ✅ Sync code files (rsync with exclusions)
5. ✅ Install dependencies (only if requirements.txt changed)
6. ✅ Verify deployment

## Protected Files (Never Overwritten)

- `plombery.db` - Production database
- `*.json` - Generated JSON files
- `saved_data/`, `saved_pages/`, `csv_downloads/` - Cached data
- `venv/` - Virtual environment
- `src/plombery/config/config.ini` - Server config with secrets

## Deployment Triggers

- ✅ Push to main/master
- ✅ Pull request merged to main/master
- ✅ New release published
- ✅ Manual workflow dispatch

## Troubleshooting

**SSH connection failed:**
```bash
ssh -i ~/.ssh/github_actions_key ubuntu@193.123.94.116
```

**Check server logs:**
```bash
ssh ubuntu@193.123.94.116 "cd /home/ubuntu/plombery && ls -la"
```

**View deployment logs:**
- Go to **Actions** tab in GitHub
- Click on latest workflow run
- Review each step's logs

## Need Help?

See [DEPLOYMENT.md](./DEPLOYMENT.md) for detailed documentation.

## Service Status

Check if the Plombery service is running:

```bash
ssh ubuntu@193.123.94.116 "systemctl status plomberly.service"
```

View live logs:

```bash
ssh ubuntu@193.123.94.116 "journalctl -u plomberly.service -f"
```
