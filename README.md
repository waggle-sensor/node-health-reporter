#

```sh
kubectl create secret generic node-health-reporter-config \
  --from-literal=SLACK_TOKEN=... \
  --from-literal=GOOGLE_SHEET_URL=...
```
