# Conduit Connector for Adobe Marketo

Marketo source connector for [Conduit](https://conduit.com) which pulls and syncs the **`Leads(People)`** object from [Marketo Engage](https://marketo.com).

### Configuration

The config passed to `Configure` can contain the following fields.
| name | part of | description | required | default value | example |
|------|---------|-------------|----------|---------------|---------|
|`clientID`|source|The Client ID for Marketo Instance|true|NONE| 1de3017c-fe42-4f20-8013-798678c956a9 |
|`clientSecret`|source|The Client Secret for Marketo Instance|true|NONE|ZZZv0Mev29vNm5vIyMwTa43lioVoBT7N|
|`clientEndpoint`|source|The Endpoint for Marketo Instance|true|NONE| https://\<instance\>.mktorest.com |
|`pollingPeriod`|source|Polling time for CDC mode. Less than 10s is not recommended |false|`1m`| `10s`, `1m`, `5m`, `10m`, `30m`, `1h` |
|`snapshotInitialDate`|source|The date from which the snapshot iterator initially starts getting records.|false|Creation date of the oldest record.|`2006-01-02T15:04:05Z07:00`|
|`fields`|source|comma seperated fields to fetch from Marketo Leads|false|`id, createdAt, updatedAt, firstName, lastName, email`| `company, jobTitle, phone, personSource` etc... |

> Note: By default **`id, createdAt, updatedAt`** is prepended to `fields` config. So no need to add that explictly. For eg: if you want to request `email, company, phone` fields, then it will be requested as **`id, createdAt, updatedAt, email, company, phone`**

## Source

Marketo source connector connects to Marketo instance through the REST API with provided configuration, using `clientID` and `clientSecret`. Once connector is started `Configure` method is called to parse configurations and validate them. After that `Open` method is called to establish connection to Marketo instance with provided position. Once connection is established `Read` method is called which calls current iterator's `Next` method to fetch next record. `Teardown` is called when connector is stopped.

### Snapshot Iterator

Snapshot iterator is used first to extract bulk data from Marketo instance. [Bulk Lead Extract API](https://developers.marketo.com/rest-api/bulk-extract/bulk-lead-extract/) is used with `createdAt` filter which permits datetime ranges up to 31 days, so we will need to run multiple jobs and combine the results. In order to get started we need to find the oldest lead created in the instance. To know the date [querry all folders](https://developers.marketo.com/rest-api/assets/folders/#browse) with maxdepth of 1 which will give us a list of all the top-level folders in the instance. Then collecting `createdAt` dates, parse them, and find the oldest date. This method works because some default, top-level folders are created with the instance and no leads could be created before then. `fields` from config also requested along with `createdAt` filter. To find the available fields for your target instance using the [Describe Lead 2 endpoint](https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Leads/describeUsingGET_6) which return an exhaustive list including both standard and custom fields.

**Exporting Job involves 4 APIS**

- Create a Job -> `/bulk/v1/leads/export/create.json`
- Enqueue a Job ->`/bulk/v1/leads/export/{{exportID}}/enqueue.json`
- Wait for Job to Complete -> `/bulk/v1/leads/export/{{exportID}}/status.json`
- Get Your Leads -> `/bulk/v1/leads/export/{{exportID}}/file.json`

After each cycle, obtained records will be flushed to conduit. Once all cycles(export jobs) are completed, connector switches to CDC mode.

### Change Data Capture Iterator

Once Snapshot iterator is completed, connector automatically switches to CDC iterator. CDC events are captured using two REST endpoints, [Get Lead Changes](https://developers.marketo.com/documentation/rest/get-lead-changes/), [Get Lead by Id](https://developers.marketo.com/documentation/rest/get-lead-by-id/). In CDC we are intrested in `New Lead (12)` and `Change Data Value (13)` events. Hence once done with [Get Lead Changes](https://developers.marketo.com/documentation/rest/get-lead-changes/) api, we filter for these `activityTypeId` 12 and 13. Once we have list of changed leads ID's, we'll query each leads with [Get Lead by Id](https://developers.marketo.com/documentation/rest/get-lead-by-id/) API to get the changed data for leads. [Deleted Leads](https://developers.marketo.com/rest-api/endpoint-reference/lead-database-endpoint-reference/#!/Activities/getDeletedLeadsUsingGET) API is used in order to capture the delete events. Output record will have a metadata of "action":"delete" to handle deletions by Conduit destination connector. No metadata is added for other CDC events such as New leads and Update leads.
From config `pollingPeriod` will be used to poll CDC events.

### Position Handling

| Name      | type              | desc                       |
| --------- | ----------------- | -------------------------- |
| Key       | string            | unique `id` for the record |
| CreatedAt | time.Time         | UTC time                   |
| UpdatedAt | time.Time         | UTC time                   |
| Type      | IteratorType(int) | 0=snapshot(default), 1=CDC |

### To build

Run `make build` to build the connector.

### Testing

Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

### Known Issues & Limitations

- In snapshot mode, the total amount of data that you can export from Marketo is limited to 500MB per day unless you have purchased a higher data limit. This 500MB limit resets daily at 12:00AM CST. Once the limit is hit pipeline stops with error. In order to pull rest of the records you need to run the pipeline again next day.
- Concurrency Limit: Maximum of 10 concurrent API calls.
- Rate Limit: API access per instance limited to 100 calls per 20 seconds.
- Daily Quota: Subscriptions are allocated 50,000 API calls per day (which resets daily at 12:00AM CST). You can increase your daily quota through your account manager.
- The connector is able to send record's `Key` as `sdk.RawData` only.
- **The integration tests are currently not configured:**. Signing up for a trial/sandbox account is relatively complex and since we use Marketo APIs here to fetch data the integration tests are not configured.

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=0d3bd3a2-ebd0-4fa7-840a-26f4fc15c2e9)