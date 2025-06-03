# digital_ingest_trigger
Invokes AWS Elastic Container Service (ECS) tasks based on SQS and S3 messages.

## Getting Started

With [git](https://git-scm.com/) installed, pull down the source code and move into the newly created directory:

```
git clone https://github.com/RockefellerArchiveCenter/digital_ingest_trigger.git
cd digital_ingest_trigger
```

## Usage

This repository is intended to be deployed as a Lambda script in AWS infrastructure.

### Expected Message Format

The script is designed to consume message from an AWS Simple Queue Service (SQS) queue. These messages are expected have the following attributes:
- `package_id` - an identifier associated with the package
- `service` - the service which produced the message
- `requested_status` - the desired status for the service (`STARTED`)

### Configured Actions

The script takes the following actions:
- S3 events:
    - PutObject events trigger the [`digital_ingest_discovery`](https://github.com/RockefellerArchiveCenter/digital_ingest_discovery) ECS task.
- SNS events:
    - Messages with a `requested_status` of `STARTED` result in the triggering of the service identified
    in the `service` attribute with the `package_id` passed into the envirohment.

## License

This code is released under the MIT License.

## Contributing

This is an open source project and we welcome contributions! If you want to fix a bug, or have an idea of how to enhance the application, the process looks like this:

1. File an issue in this repository. This will provide a location to discuss proposed implementations of fixes or enhancements, and can then be tied to a subsequent pull request.
2. If you have an idea of how to fix the bug (or make the improvements), fork the repository and work in your own branch. When you are done, push the branch back to this repository and set up a pull request. Automated unit tests are run on all pull requests. Any new code should have unit test coverage, documentation (if necessary), and should conform to the Python PEP8 style guidelines.
3. After some back and forth between you and core committers (or individuals who have privileges to commit to the base branch of this repository), your code will probably be merged, perhaps with some minor changes.

This repository contains a configuration file for git [pre-commit](https://pre-commit.com/) hooks which help ensure that code is linted before it is checked into version control. It is strongly recommended that you install these hooks locally by installing pre-commit and running `pre-commit install`.

## Tests

New code should have unit tests. Tests can be run using [tox](https://tox.readthedocs.io/).
