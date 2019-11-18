import requests
from contextlib import closing
import csv


class Settings:
    url: str = "{{PUBLIC_REPOSITORY_URL}}"
    delimiter: str = ";"
    quote_char: str = '"'
    decode: str = 'utf-8'
    limit: int = 0
    skip: int = 0


def error_handle(line_num, settings):
    print("ERROR at line: " + str(line_num) + " file: " + settings.url)


def build_settings(request):
    settings = Settings()
    request_json = request.get_json()

    if request.args and 'file' in request.args:
        settings.url = settings.url + request.args.get('file')

    elif request_json and 'file' in request_json:
        settings.url = settings.url + request_json['file']

    else:
        settings.url = None

    if request.args and 'delimiter' in request.args:
        settings.delimiter = request.args.get('delimiter')

    elif request_json and 'delimiter' in request_json:
        settings.delimiter = request_json['delimiter']

    if request.args and 'limit' in request.args:
        settings.limit = int(request.args.get('limit'))

    elif request_json and 'limit' in request_json:
        settings.limit = int(request_json['limit'])

    if request.args and 'skip' in request.args:
        settings.skip = int(request.args.get('skip'))

    elif request_json and 'skip' in request_json:
        settings.skip = int(request_json['skip'])

    return settings


def main(request):
    settings = build_settings(request)

    if settings.url is None:
        return "sorry, but you should send a filename as 'file' parameter"

    header = []
    arr = []

    with closing(requests.get(settings.url, stream=True)) as r:
        f = (line.decode(settings.decode) for line in r.iter_lines())
        reader = csv.reader(f, delimiter=settings.delimiter, quotechar=settings.quote_char)

        for row in reader:

            col_num = len(row)

            if 1 == reader.line_num:
                header = row

            elif settings.skip > reader.line_num - 2:
                continue

            elif 0 < (settings.limit + settings.skip) < reader.line_num - 1:
                break

            elif col_num > 0:
                obj = {}
                for i in range(col_num):
                    obj[header[i]] = str(row[i])

                arr.append(obj)

            else:
                error_handle(reader.line_num, settings)

    return str(arr)
