import requests
import time
from concurrent.futures import ThreadPoolExecutor

# This needs to be a unique value not used with the omdb api 
prefix = "my_mail_prefix1"

# The api doesn't like too many requests at once
# Sometimes it returns the same key multiple times
# When reading the key file only the unique keys should be read.
# Some keys are also returned as "!DOCTYPE html>"
for i in range(50):
    n = 50
    save_file = "../secrets/omdb_api_keys.secret"
    # This url might update if they change how API keys are generated.
    # Manually generate one key on the http://www.omdbapi.com website and record the network request to find it.
    url_key = "http://www.omdbapi.com/apikey.aspx?__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE=%2FwEPDwUKLTIwNDY4MTIzNQ9kFgYCAQ9kFggCAQ8QDxYCHgdDaGVja2VkaGRkZGQCAw8QDxYCHwBnZGRkZAIFDxYCHgdWaXNpYmxlaGQCBw8WAh8BZ2QCAg8WAh8BaGQCAw8WAh8BaGQYAQUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgMFC3BhdHJlb25BY2N0BQtwYXRyZW9uQWNjdAUIZnJlZUFjY3Q70lz2duvFWpi7KjqMV5Bfeiu%2F8XZfNDl5GeZkBw9M5A%3D%3D&__VIEWSTATEGENERATOR=5E550F58&__EVENTVALIDATION=%2FwEdAAjUveXue3qLFY2FjQgdgUYHmSzhXfnlWWVdWIamVouVTzfZJuQDpLVS6HZFWq5fYphdL1XrNEjnC%2FKjNya%2Bmqh8hRPnM5dWgso2y7bj7kVNLSFbtYIt24Lw6ktxrd5Z67%2F4LFSTzFfbXTFN5VgQX9Nbzfg78Z8BXhXifTCAVkevd%2FM3%2B1LlDH%2BEpR4wCViK8IATPCorai%2FJA3cV0kG3vG6P&at=freeAcct&Email2={0}%40mailix.xyz&FirstName=usdhfiusdhfiu&LastName=uhsdfiuhsdfiuh&TextArea1=shdfishdfsdjf&Button1=Submit"
    url_key = "http://www.omdbapi.com/apikey.aspx?__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE=%2FwEPDwUKLTIwNDY4MTIzNQ9kFgYCAQ9kFggCAQ8QDxYCHgdDaGVja2VkaGRkZGQCAw8QDxYCHwBnZGRkZAIFDxYCHgdWaXNpYmxlaGQCBw8WAh8BZ2QCAg8WAh8BaGQCAw8WAh8BaGQYAQUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgMFC3BhdHJlb25BY2N0BQtwYXRyZW9uQWNjdAUIZnJlZUFjY3Q70lz2duvFWpi7KjqMV5Bfeiu%2F8XZfNDl5GeZkBw9M5A%3D%3D&__VIEWSTATEGENERATOR=5E550F58&__EVENTVALIDATION=%2FwEdAAjUveXue3qLFY2FjQgdgUYHmSzhXfnlWWVdWIamVouVTzfZJuQDpLVS6HZFWq5fYphdL1XrNEjnC%2FKjNya%2Bmqh8hRPnM5dWgso2y7bj7kVNLSFbtYIt24Lw6ktxrd5Z67%2F4LFSTzFfbXTFN5VgQX9Nbzfg78Z8BXhXifTCAVkevd%2FM3%2B1LlDH%2BEpR4wCViK8IATPCorai%2FJA3cV0kG3vG6P&at=freeAcct&Email2={0}%40mailix.xyz&FirstName=uisdhf&LastName=siudhfiu&TextArea1=sdiufhsf&Button1=Submit"
    # The api might change but you should be able to find it on https://mailcare.io/
    url_get_emails = "https://mailix.xyz/api/emails?page=1&limit={0}&sender=donotreply%40omdbapi.com&unread=true".format(n)
    url_open_email = "https://mailix.xyz/api/emails/{0}"

    def request_key(j):
        r = requests.get(url_key.format(f"{prefix}{j}")) 

    print(f"Requesting {n} keys")    
    with ThreadPoolExecutor(max_workers=n) as executor:
        executor.map(request_key, range(i*n, i*n+n))

    print("Waiting for email server to get api keys")
    time.sleep(15)

    r = requests.get(url_get_emails)
    email_ids = [e["id"] for e in r.json()["data"]]

    print(f"Found {len(email_ids)} emails")

    keys = []
    def get_and_activate_key(id):
        r = requests.get(url_open_email.format(id), headers={"Accept": "text/plain"})
        s = r.text.split("\n")
        keys.append(s[0][s[0].find(": ")+2:])
        activate_url = s[6][s[6].find(": ")+2:]
        requests.get(activate_url)

    with ThreadPoolExecutor(max_workers=len(email_ids)) as executor:
        executor.map(get_and_activate_key, email_ids)

    print("Saving file")

    with open(save_file, "a") as f:
        for key in keys:
            f.write(key + "\n")