"""Reset and run the three Fabric demonstrations."""

import time
from email.utils import formatdate
from urllib.request import Request, urlopen

from simple_data import sales_csv


def upload(context, demo, filename, content):
    settings = context["config"][demo]
    path = settings["landing_path"].strip("/")
    url = (f"https://onelake.blob.fabric.microsoft.com/{settings['workspace_id']}/"
           f"{settings['lakehouse_id']}/{path}/{filename}")
    request = Request(url, data=content, method="PUT", headers={
        "Authorization": "Bearer " + context["get_token"]("https://storage.azure.com/"),
        "x-ms-version": "2021-08-06",
        "x-ms-date": formatdate(usegmt=True),
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "text/csv",
    })
    with urlopen(request, timeout=60) as response:
        if response.status != 201:
            raise RuntimeError(f"OneLake upload failed with HTTP {response.status}")


def _stop_all(context, modules, failed):
    errors = []
    for name, module in modules:
        try:
            module.stop(context, failed=failed)
        except Exception as error:
            errors.append(f"{name}: {type(error).__name__}: {error}")
    if errors:
        raise RuntimeError("Unable to stop demo activity: " + "; ".join(errors))


def run(context, modules, *, send=upload, sleep=time.sleep):
    data = context["config"]["data"]
    try:
        _stop_all(context, modules, True)
        for _, module in modules:
            module.reset(context)
        for _, module in modules:
            module.start(context)
        for number in range(1, data["file_count"] + 1):
            if number > 1:
                sleep(data["interval_seconds"])
            content = sales_csv(data, number)
            filename = f"sales_{number:04d}.csv"
            send(context, "demo-1", filename, content)
            send(context, "demo-3", filename, content)
            dict(modules)["demo-2"].submit_batch(context, content)
            print(f"Published {filename} to the three demos.", flush=True)
        _stop_all(context, modules, False)
    except Exception as error:
        try:
            _stop_all(context, modules, True)
        except Exception as stop_error:
            raise RuntimeError(f"Demo failed: {error}; cleanup failed: {stop_error}") from error
        raise
