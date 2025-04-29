# TODO

## Project tasks
- [x] Overhaul the README and remove out-of-date, unnecessary content (partly done, but still more to do on this one IMO)
- [x] Remove unnecessary scripts like set-sample-env.sh and run-tests
- [x] Remove 'modes' from tests, make everything use DynamoDB local
- [x] Migrate to clojure.test 
- [x] Release 1.10 GA
- [x] Update the Changelog for 1.10 
- [x] Move from Travis CI to GitHub Actions
- [ ] Add a GitHub Action for release
 
## Features
- [x] Transactions
- [ ] Lazy paging

### Table configuration
- [x] TTL
- [ ] Backup
- [ ] Point-in-time recovery
- [ ] Encryption type (+ KMS)
- [ ] Auto-scaling
- [x] On-Demand capacity  
- [ ] Global tables
- [ ] Triggers
- [ ] Tags

## AWS SDK 2 migration

- [x] Client/credentials
- [ ] Non-blocking operations support
- [ ] Readme documentation

### Migrate faraday 1 api

- [ ] query
- [ ] update-ttl
- [ ] ensure-table
- [ ] batch-write-item
- [ ] transact-write-items
- [ ] clj-item->db-item
- [ ] describe-ttl
- [ ] serialize
- [ ] get-stream-records
- [ ] create-table
- [ ] scan-parallel
- [ ] batch-get-item
- [ ] freeze
- [ ] AsMap
- [ ] update-table
- [ ] get-item
- [ ] without-attr-multi-vs
- [ ] with-attr-multi-vs
- [ ] transact-get-items
- [ ] update-item
- [ ] put-item
- [ ] items-by-attrs
- [ ] delete-item
- [ ] *attr-multi-vs?*
- [ ] scan-lazy-seq
- [ ] delete-table
- [ ] describe-table
- [ ] ensure-ttl
- [ ] list-streams
- [ ] list-tables
- [ ] shard-iterator
- [ ] ISerializable
- [ ] describe-stream
- [ ] ex
- [ ] db-item->clj-item
- [ ] with-thaw-opts
- [ ] as-map
- [ ] scan
- [ ] remove-empty-attr-vals
