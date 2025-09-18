---
name: Bug report
about: Create a report to help us improve
title: '[BUG] '
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Error Output**
If applicable, paste the error output or stack trace:
```
Error output here
```

**Environment (please complete the following information):**
 - OS: [e.g. macOS, Ubuntu 20.04]
 - Python version: [e.g. 3.11.0]
 - Java version: [e.g. OpenJDK 11]
 - Spark version: [e.g. 3.5.0]
 - Delta Lake version: [e.g. 3.2.1]
 - Deployment: [e.g. Local, GCP, Docker]

**Configuration**
Please share your configuration (remove sensitive information):
```yaml
# config/config.yaml (sanitized)
project_name: "maritime-activity-reports-cdc"
environment: "dev"
# ... other relevant config
```

**Logs**
If applicable, add relevant log output:
```
Log output here
```

**Additional context**
Add any other context about the problem here.

**Health Check Output**
If possible, include the output of:
```bash
pdm run maritime-reports health-check
```
