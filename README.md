# Hub

![Hub](https://github.com/rhiaqey/hub/actions/workflows/pr_merge.yml/badge.svg)

Early development stage

## Redis streams example

[Redis streams](https://github.com/redis-rs/redis-rs/blob/main/redis/examples/streams.rs)

## Settings

```json
{
  "Security": {
    "ApiKeys": [
      {
        "ApiKey": "some_api_key",
        "Host": "some_host",
        "IPs": {
          "Blacklisted": [
            "192.168.0.0"
          ]
        }
      }
    ]
  }
}
```
