# cb-wrapper-dynamodb

## Using this library in a different project

### `pip`

In general, any library can be directly installed with
```shell
python -m pip install <requirement specifier>
```

Alternatively, multiple `<requirement specifier>`s can be listed on a project's `requirements.txt` file:
```bash
# requirements.txt
<requirement specifier>
<requirement specifier>
...
```
which can be all installed by running
```bash
python -m pip install -r requirements.txt
```

There are several ways to write the `<requirement specifier>` (see [here](https://stackoverflow.com/questions/16584552/how-to-state-in-requirements-txt-a-direct-github-source)).
At the time of writing, the safest way to reference a specific version is to use the **commit hash** (e.g. `23a3cf3`):
```
git+https://github.com/ConsiliumBots/cb-wrapper-dynamodb@23a3cf3#egg=dynamodb
```

This specifier requires that the machine has access to the repo. For CI/CD, it might be better to have an access token stored in an environment variable
```
git+https://${GH_USER}:${GH_TOKEN}@github.com/ConsiliumBots/cb-wrapper-dynamodb@23a3cf3#egg=dynamodb
```