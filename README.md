# api-preprocessor

## Overview

## Behavior

## Environment Variables

## Local Development
### Testing with dev CloudRun instance

Proxy your local machine to the CloudRun gateway 
(this both proxies and shims w/ an auth interceptor between your local machine and GCP):
```bash
gcloud run services proxy api-preprocessor-dev
```

Hit the proxied localhost address, as you would the remote address.
```bash
curl -X POST -H "Content-Type: application/json" -d '{}' http://localhost:8080/run
```


## Deploy