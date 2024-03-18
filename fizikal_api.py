from typing import Any
import requests
import shelve
import sys
import utils
import json
import datetime


class FizikalAPI:
    def __init__(self, fizikal_config={}, persistent_storage="", mock=False):
        self.base_url = fizikal_config.get("api_base_url", "")
        self.phone_number = fizikal_config.get("phone_number", "")
        if not self.base_url:
            raise Exception("FizikalAPI: Base URL not specified")
        elif not self.base_url.startswith("https://"):
            raise Exception("FizikalAPI: Base URL must be HTTPS")
        elif not self.phone_number:
            raise Exception("FizikalAPI: Phone number not specified")
        elif not persistent_storage:
            raise Exception("FizikalAPI: Persistent storage not specified")

        for key in fizikal_config:
            if not fizikal_config[key]:
                raise Exception(f"FizikalAPI: {key} not specified in config")

        self.mock = mock
        self.config = fizikal_config
        self.persistent_storage = persistent_storage
        self.http_log = open(persistent_storage + "/http_requests.log", "a")

        self.cache = shelve.open(persistent_storage + "/fizikal_api_cache_" + datetime.datetime.now().strftime("%m/%d/%Y").replace('/','_'))
        if not "refresh_token" in self.cache:  # First time running
            # Check if running interactive or not
            if sys.stdin.isatty():
                print(
                    f"First time / No refresh token found, Logging in with phone {self.phone_number}"
                )
                self.login()
            else:
                raise Exception(
                    "FizikalAPI: No refresh token found. and not running interactive."
                )

    def get_mock_response(self, endpoint: str) -> requests.Response:
        normalized_endpoint = endpoint.replace("/", "_")
        with open(f"mocks/{normalized_endpoint}.json", "r") as file:
            response_data = json.load(file)
            response = requests.Response()
            response.status_code = 200
            response._content = json.dumps(response_data).encode("utf-8")
            return response

    def login_request(self):
        """
        GET /app/v1/login/Authentication?OrganizationId=1&companyId=230&branchId=14&sourceId=1&authenticationTypeId=1&value=<phone> HTTP/1.1
        user-agent: Dart/2.19 (dart:io)
        deviceid: 1d6dd95a02329496
        version: 70.0.5298
        accept-encoding: gzip
        host: api.fizikal.co.il
        authorization: Bearer null

        HTTP/1.1 200 OK
        Date: Thu, 30 Nov 2023 18:21:59 GMT
        Content-Type: application/json; charset=utf-8
        Content-Length: 33
        Connection: close
        Cache-Control: no-cache
        Pragma: no-cache
        Expires: -1
        X-AspNet-Version: 4.0.30319
        CF-Cache-Status: DYNAMIC
        Report-To: {"endpoints":[{"url":"https:\/\/a.nel.cloudflare.com\/report\/v3?s=z0PZBQ1RWgrm9ogb5lVQEtCW%2F5Q2Q%2BwgO5FJq3AXDXr7sDh%2FUAuZbMw7Fd3jSjeBDJswCYLmCOICKNKvA8Zca8URPdXz3v5VdDRVQ4jm6Uzw08MhR%2BA%2FP6K542Kf318oFp2S"}],"group":"cf-nel","max_age":604800}
        NEL: {"success_fraction":0,"report_to":"cf-nel","max_age":604800}
        Server: cloudflare
        CF-RAY: 82e51cf37fb202a2-ORD

        {"success":true,"statusCode":200}
        """
        endpoint = "/app/v1/login/Authentication"
        params = {
            "OrganizationId": self.config["OrganizationId"],
            "companyId": self.config["companyId"],
            "branchId": self.config["branchId"],
            "sourceId": self.config["sourceId"],
            "authenticationTypeId": self.config["authenticationTypeId"],
            "value": self.phone_number,
        }

        if not "device_id" in self.cache:
            self.cache["device_id"] = utils.generate_device_id()

        headers = {
            "user-agent": "Dart/2.19 (dart:io)",
            "deviceid": self.cache["device_id"],
            "version": "70.0.5298",
            "accept-encoding": "gzip",
            "host": self.base_url.strip("https://").strip("http://").strip("/"),
        }
        response = self.send_request(endpoint, "GET", params=params, headers=headers)
        if response.status_code != 200:
            raise Exception(
                f"FizikalAPI: Login failed with status code {response.status_code}. Message: {response.text}"
            )
        else:
            response_json = response.json()
            if not "success" in response_json or not response_json["success"] == True:
                raise Exception(
                    f"FizikalAPI: Login failed with status code {response.status_code}. Message: {response.text}"
                )

    def login_verification(self, verification_code: str):
        """
        GET /app/v1/login/Verification?verificationCode=<verification_code>&OrganizationId=1&companyId=230&branchId=14&sourceId=1&authenticationTypeId=1&value=<phone_number> HTTP/1.1
        user-agent: Dart/2.19 (dart:io)
        deviceid: 1d6dd95a02329496
        version: 70.0.5298
        accept-encoding: gzip
        host: api.fizikal.co.il
        authorization: Bearer null

        HTTP/1.1 200 OK
        Date: Thu, 30 Nov 2023 18:22:14 GMT
        Content-Type: application/json; charset=utf-8
        Content-Length: 998
        Connection: close
        Cache-Control: no-cache
        Pragma: no-cache
        Expires: -1
        X-AspNet-Version: 4.0.30319
        CF-Cache-Status: DYNAMIC
        Report-To: {"endpoints":[{"url":"https:\/\/a.nel.cloudflare.com\/report\/v3?s=5gbMUKzmG0qRRM2mmjDxdktoB2iWFYZIAS9h%2BL4ylB%2FgH2DcBGyvjC6CNuKol4IqEMigYhY0GcdL0LSGRII7j74t1u8HkwjlGrR%2FLr0jNWVlZsb9k9PtPyiU7na%2FZD8CLfuv"}],"group":"cf-nel","max_age":604800}
        NEL: {"success_fraction":0,"report_to":"cf-nel","max_age":604800}
        Server: cloudflare
        CF-RAY: 82e51d55cf722bcc-ORD

        {"success":true,"statusCode":200,"data":{"accessToken":"<access_token>","refreshToken":"<refresh_token>","isRequiredTermForm":false,"branches":[{"name":"חשמונאים","loginCustomerId":28336,"loginCompanyId":230,"loginBranchId":14,"accessToCustomerId":28336,"accessToCompanyId":230,"accessToBranchId":14,"isSelected":true,"customerTypeId":1}]}}
        """
        endpoint = "/app/v1/login/Verification"
        params = {
            "OrganizationId": self.config["OrganizationId"],
            "companyId": self.config["companyId"],
            "branchId": self.config["branchId"],
            "sourceId": self.config["sourceId"],
            "authenticationTypeId": self.config["authenticationTypeId"],
            "value": self.phone_number,
            "verificationCode": verification_code,
        }
        headers = {
            "user-agent": "Dart/2.19 (dart:io)",
            "deviceid": self.cache["device_id"],
            "version": "70.0.5298",
            "accept-encoding": "gzip",
            "host": self.base_url.strip("https://").strip("http://").strip("/"),
        }
        response = self.send_request(endpoint, "GET", params=params, headers=headers)
        if response.status_code != 200:
            raise Exception(
                f"FizikalAPI: Login verification failed with status code {response.status_code}. Message: {response.text}"
            )
        else:
            response_json = response.json()
            if not "success" in response_json or not response_json["success"] == True:
                raise Exception(
                    f"FizikalAPI: Login verification failed with status code {response.status_code}. Message: {response.text}"
                )
            else:
                self.cache["refresh_token"] = response_json.get("data", {}).get(
                    "refreshToken"
                )
                self.cache["access_token"] = response_json.get("data", {}).get(
                    "accessToken"
                )
                self.cache["branches"] = response_json.get("data", {}).get("branches")
                self.cache["customer_id"] = (
                    response_json.get("data", {})
                    .get("branches", [{}])[0]
                    .get("loginCustomerId")
                )
                self.cache["company_id"] = (
                    response_json.get("data", {})
                    .get("branches", [{}])[0]
                    .get("loginCompanyId")
                )
                self.cache["branch_id"] = (
                    response_json.get("data", {})
                    .get("branches", [{}])[0]
                    .get("loginBranchId")
                )

    def renew_access_token(self):
        """
        POST /app/v1/login/Token HTTP/1.1
        user-agent: Dart/2.19 (dart:io)
        content-type: application/x-www-form-urlencoded; charset=utf-8
        accept-encoding: gzip
        content-length: 158
        host: api.fizikal.co.il

        companyId=230&branchId=14&organizationId=1&refreshToken=<refresh_token>

        HTTP/1.1 200 OK
        Date: Thu, 30 Nov 2023 18:38:31 GMT
        Content-Type: application/json; charset=utf-8
        Content-Length: 677
        Connection: close
        Cache-Control: no-cache
        Pragma: no-cache
        Expires: -1
        X-AspNet-Version: 4.0.30319
        CF-Cache-Status: DYNAMIC
        Report-To: {"endpoints":[{"url":"https:\/\/a.nel.cloudflare.com\/report\/v3?s=eLry8UhcqBZ6ELuSF9VppCOJjRMak6UWg%2FotRUEyhlrdDLOS%2B00kg2VlrecM9gwgSwQIR%2F%2Bu9TAwnVmuXJEtLJ8cpeUkPSCOKhuh7I0spE9WtkNsfnfMXB9tyBVJ%2FAmSmm4M"}],"group":"cf-nel","max_age":604800}
        NEL: {"success_fraction":0,"report_to":"cf-nel","max_age":604800}
        Server: cloudflare
        CF-RAY: 82e53531bbc1631e-ORD

        {"success":true,"statusCode":200,"data":{"accessToken":"<JWT_ACCESS_TOKEN>","isRequiredTermForm":false}}

        """
        if not "refresh_token" in self.cache:
            self.login()
        endpoint = "/app/v1/login/Token"
        headers = {
            "user-agent": "Dart/2.19 (dart:io)",
            "content-type": "application/x-www-form-urlencoded; charset=utf-8",
            "accept-encoding": "gzip",
            "host": self.base_url.strip("https://").strip("http://").strip("/"),
        }
        data = {
            "companyId": self.config["companyId"],
            "branchId": self.config["branchId"],
            "organizationId": self.config["OrganizationId"],
            "refreshToken": self.cache["refresh_token"],
        }
        response = self.send_request(endpoint, "POST", headers=headers, data=data)
        if response.status_code != 200:
            raise Exception(
                f"FizikalAPI: Renew access token failed with status code {response.status_code}. Message: {response.text}"
            )
        else:
            response_json = response.json()
            if not "success" in response_json or not response_json["success"] == True:
                raise Exception(
                    f"FizikalAPI: Renew access token failed with status code {response.status_code}. Message: {response.text}"
                )
            else:
                try:
                    self.cache["access_token"] = response_json.get("data", {}).get(
                        "accessToken"
                    )
                except Exception as e:
                    raise Exception(
                        f"FizikalAPI: Renew access token failed with status code {response.status_code}. Message: {response.text}"
                    )

    def login(self):
        self.login_request()
        sms_code = ""
        while not sms_code or not sms_code.isdigit():
            sms_code = input("Please enter the code you received via SMS: ")
        self.login_verification(sms_code)

    def unregister_class(self, class_id: int) -> bool:
        """
        POST /app/v1/classes/registration/remove HTTP/1.1
        user-agent: Dart/2.19 (dart:io)
        deviceid: 1d6dd95a02329496
        version: 70.0.5298
        accept-encoding: gzip
        content-length: 18
        host: api.fizikal.co.il
        authorization: Bearer <Token>
        content-type: application/json; charset=utf-8



        {"id":"791260333"}

        HTTP/1.1 200 OK
        Date: Thu, 30 Nov 2023 18:23:04 GMT
        Content-Type: application/json; charset=utf-8
        Content-Length: 528
        Connection: close
        Cache-Control: no-cache
        Pragma: no-cache
        Expires: -1
        X-AspNet-Version: 4.0.30319
        CF-Cache-Status: DYNAMIC
        Report-To: {"endpoints":[{"url":"https:\/\/a.nel.cloudflare.com\/report\/v3?s=Dnd7oSkMD%2F3s04VIOv8jVJZTZUJNpQNa%2B56cXmjRD1Tzmaf7UHlYUyO0KQ967AYHgOGMNGNrFNyp%2FBnMABXxVzexoCXiH%2BAqqIRS0zLhZXLPKMOYGiVWTkVydrcHyd4GgskL"}],"group":"cf-nel","max_age":604800}
        NEL: {"success_fraction":0,"report_to":"cf-nel","max_age":604800}
        Server: cloudflare
        CF-RAY: 82e51e806e06e27f-ORD



        {"success":true,"statusCode":200,"data":{"class":{"id":2437,"contractId":0,"purchaseId":0,"day":"שישי","date":"01/12","dateRequest":"2023-12-01","startTime":"07:45","endTime":"08:30","replaceTime":"07:45","description":"Functional Training","instructorId":328,"instructorName":"חן מתיאס","maxParticipants":16,"totalParticipants":9,"groupsIds":[0,2],"dayPartId":1,"isFavorite":false,"locationName":"אולם ייעודי","customerStatusId":1,"action":{"text":"הרשמה","name":"AddRegistration"}},"actionStatus":1}}
        """
        endpoint = "/app/v1/classes/registration/remove"
        headers = {
            "user-agent": "Dart/2.19 (dart:io)",
            "deviceid": self.cache["device_id"],
            "version": "70.0.5298",
            "accept-encoding": "gzip",
            "host": self.base_url.strip("https://").strip("http://").strip("/"),
            "content-type": "application/json; charset=utf-8",
        }
        data = {"id": class_id}
        response = self.send_authenticated_request(
            endpoint, "POST", headers=headers, data=json.dumps(data)
        )
        if response.status_code != 200:
            raise Exception(
                f"FizikalAPI: Unregister class failed with status code {response.status_code}. Message: {response.text}"
            )
        return True

    def register_class(self, class_id: int, class_date: str) -> dict:
        """
        GET /app/v1/classes/registration/add?contractId=0&purchaseId=0&classId=2516&classDate=2023-12-01 HTTP/2
        Host: api.fizikal.co.il
        User-Agent: Dart/2.19 (dart:io)
        Deviceid: 1d6dd95a02329496
        Version: 70.0.5298
        Accept-Encoding: gzip
        Authorization: Bearer <JWT_TOKEN>

        HTTP/2 200 OK
        Date: Thu, 30 Nov 2023 19:04:27 GMT
        Content-Type: application/json; charset=utf-8
        Cache-Control: no-cache
        Pragma: no-cache
        Expires: -1
        X-Aspnet-Version: 4.0.30319
        Cf-Cache-Status: DYNAMIC
        Report-To: {"endpoints":[{"url":"https:\/\/a.nel.cloudflare.com\/report\/v3?s=DUlCvty%2FpFgwR%2Fc%2FI%2BpGJboMiqLJJKQ8K308EjZTIpNp%2Fc3NxgDaZDX1utjeb3NILDub%2BUMKpgER5%2FSX%2Fng4GSRBtSOt%2FshZXXp%2BekKJ8keXxEUPDyEIE8xTZG5At5J0BHar"}],"group":"cf-nel","max_age":604800}
        Nel: {"success_fraction":0,"report_to":"cf-nel","max_age":604800}
        Server: cloudflare
        Cf-Ray: 82e55b320cb11cbb-FRA

        {"success":true,"statusCode":200,"data":{"class":{"id":2516,"contractId":0,"purchaseId":0,"day":"שישי","date":"01/12","dateRequest":"2023-12-01","startTime":"10:00","endTime":"10:45","replaceTime":"10:00","description":"Spin","instructorId":377,"instructorName":"הדס סלוק","maxParticipants":20,"totalParticipants":12,"groupsIds":[0,5,15],"dayPartId":1,"isFavorite":false,"locationName":"ספינינג","registrationId":791266385,"customerStatusId":2,"action":{"text":"ביטול","name":"RemoveRegistration"}},"actionStatus":1}}
        """
        endpoint = "/app/v1/classes/registration/add"
        params = {
            "contractId": 0,
            "purchaseId": 0,
            "classId": class_id,
            "classDate": class_date,
        }
        headers = {
            "user-agent": "Dart/2.19 (dart:io)",
            "deviceid": self.cache["device_id"],
            "version": "70.0.5298",
            "accept-encoding": "gzip",
            "host": self.base_url.strip("https://").strip("http://").strip("/"),
        }
        response = self.send_authenticated_request(
            endpoint, "GET", params=params, headers=headers
        )
        if response.status_code != 200:
            raise Exception(
                f"FizikalAPI: Register class failed with status code {response.status_code}. Message: {response.text}"
            )

        response_json = response.json()
        if (
            not "success" in response_json
            or not response_json["success"] == True
            or response_json.get("statusCode", 0) != 200
        ):
            raise Exception(
                f"FizikalAPI: Register class failed with status code {response.status_code}. Message: {response.text}"
            )
        data = response_json.get("data", {})
        if not "class" in data:
            raise Exception(
                f"FizikalAPI: Register class failed with status code {response.status_code}. Message: {response.text}"
            )

        return data["class"]

    def remove_class(self, registration_id: int) -> dict:
        """
        POST /app/v1/classes/registration/remove HTTP/1.1
        user-agent: Dart/2.19 (dart:io)
        deviceid: 1d6dd95a02329496
        version: 70.0.5298
        accept-encoding: gzip
        content-length: 18
        host: api.fizikal.co.il
        authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJJZCI6IjI4MzM2IiwiTmFtZSI6Ikx5aHpreUpLblFhYTd3aUEzb1g1N1E9PSIsIkNvbXBhbnlJZCI6IjIzMCIsIkJyYW5jaElkIjoiMTQiLCJMb2dpbkN1c3RvbWVySWQiOiIyODMzNiIsIkxvZ2luQ29tcGFueUlkIjoiMjMwIiwiTG9naW5CcmFuY2hJZCI6IjE0IiwiQXBwQ29tcGFueUlkIjoiMjMwIiwiQXBwQnJhbmNoSWQiOiIxNCIsIkFwcFNvdXJjZUlkIjoiMSIsIkFwcEN1c3RvbWVyVHlwZUlkIjoiMSIsIkF1dGhlbnRpY2F0aW9uVHlwZUlkIjoiMSIsIkF1dGhlbnRpY2F0aW9uVmFsdWUiOiJRR1cyUTJaV0pTMG9sZ3VjM0JnanRnPT0iLCJPcmdhbml6YXRpb25JZCI6IjEiLCJuYmYiOjE3MDEzNjg1MzMsImV4cCI6MTcwMTM2OTEzMywiaWF0IjoxNzAxMzY4NTMzfQ.JzXHHUb_OzDITorEylm4BniJBhX7Pie4usHa5Q1shLA
        content-type: application/json; charset=utf-8

        {"id":"791260333"}
        """
        endpoint = "/app/v1/classes/registration/remove"
        # params = {
        #     'contractId': 0,
        #     'purchaseId': 0,
        #     'classId': class_id,
        #     'classDate': class_date
        # }
        headers = {
            "user-agent": "Dart/2.19 (dart:io)",
            "deviceid": self.cache["device_id"],
            "version": "70.0.5298",
            "accept-encoding": "gzip",
            "host": self.base_url.strip("https://").strip("http://").strip("/"),
        }
        payload = {"id": registration_id}
        response = self.send_authenticated_request(
            endpoint, "POST", params={}, headers=headers, data=payload
        )
        if response.status_code != 200:
            raise Exception(
                f"FizikalAPI: Register class failed with status code {response.status_code}. Message: {response.text}"
            )

        response_json = response.json()
        if (
            not "success" in response_json
            or not response_json["success"] == True
            or response_json.get("statusCode", 0) != 200
        ):
            raise Exception(
                f"FizikalAPI: Register class failed with status code {response.status_code}. Message: {response.text}"
            )
        data = response_json.get("data", {})
        if not "class" in data:
            raise Exception(
                f"FizikalAPI: Register class failed with status code {response.status_code}. Message: {response.text}"
            )

        return data["class"]

    def get_classes(self, delta=1) -> dict:
        """
        GET /app/v1/classes/schedule/view?date=2023-12-01%2013:22:14.421962 HTTP/1.1
        user-agent: Dart/2.19 (dart:io)
        deviceid: 1d6dd95a02329496
        version: 70.0.5298
        accept-encoding: gzip
        host: api.fizikal.co.il
        authorization: Bearer <Token>

        HTTP/1.1 200 OK
        Date: Thu, 30 Nov 2023 18:22:40 GMT
        Content-Type: application/json; charset=utf-8
        Content-Length: 21174
        Connection: close
        Cache-Control: no-cache
        Pragma: no-cache
        Expires: -1
        X-AspNet-Version: 4.0.30319
        CF-Cache-Status: DYNAMIC
        Report-To: {"endpoints":[{"url":"https:\/\/a.nel.cloudflare.com\/report\/v3?s=nE%2F2veCShJU6DxWgN3l5BBL1uGY21a1uDeQ2S4PFxp8I0NeVXlpFZQMfypjKa2Q9HZXconTVVmcATLtmhb%2BcWpkvrFCzTBf13EaWpa%2BhjiRLgenA7jQfkkmJIHUNaaZ0E2p5"}],"group":"cf-nel","max_age":604800}
        NEL: {"success_fraction":0,"report_to":"cf-nel","max_age":604800}
        Server: cloudflare
        CF-RAY: 82e51df9cc321098-ORD

        {
        "success": true,
        "statusCode": 200,
        "data": {
            "list": [
            {
                "id": 3100,
                "day": "שישי",
                "date": "01/12",
                "startTime": "07:00",
                "endTime": "07:45",
                "description": "Body Shape",
                "instructorName": "ליליה קרנדל",
                "maxParticipants": 24,
                "totalParticipants": 14,
                "locationName": "אולם תנועה",
                "action": {
                "text": "הרשמה",
                "name": "AddRegistration"
                }
            },
            {
                "id": 2437,
                "day": "שישי",
                "date": "01/12",
                "startTime": "07:45",
                "endTime": "08:30",
                "description": "Functional Training",
                "instructorName": "חן מתיאס",
                "maxParticipants": 16,
                "totalParticipants": 9,
                "locationName": "אולם ייעודי",
                "action": {
                "text": "הרשמה",
                "name": "AddRegistration"
                }
            },
            // More entries truncated for brevity...
            ]
        }
        }

        """
        date = (datetime.datetime.now() + datetime.timedelta(days=delta)).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        params = {"date": date}
        endpoint = "/app/v1/classes/schedule/view"
        response = self.send_authenticated_request(endpoint, params=params)
        response_json = response.json()
        if (
            not "success" in response_json
            or not response_json["success"] == True
            or response_json.get("statusCode", 0) != 200
        ):
            raise Exception(
                f"FizikalAPI: Get classes failed with status code {response.status_code}. Message: {response.text}"
            )

        data = response_json.get("data", {})
        if not "list" in data:
            raise Exception(
                f'FizikalAPI: Get classes did not return "list". failed with status code {response.status_code}. Message: {response.text}'
            )

        return data["list"]

    def send_authenticated_request(
        self, endpoint, method="GET", headers=None, params=None, data=None
    ):
        if self.mock:
            return self.get_mock_response(endpoint)

        if not "access_token" in self.cache:
            self.renew_access_token()
        if not headers:
            headers = {}
        headers["authorization"] = f'Bearer {self.cache["access_token"]}'
        response = self.send_request(endpoint, method, headers, params, data)
        if response.status_code == 401:
            self.renew_access_token()
            headers["authorization"] = f'Bearer {self.cache["access_token"]}'
            response = self.send_request(endpoint, method, headers, params, data)
        return response

    def send_request(
        self, endpoint, method="GET", headers=None, params=None, data=None
    ):  # TEST THIS
        if self.mock:
            return self.get_mock_response(endpoint)
        url = self.base_url + endpoint
        response = requests.request(
            method, url, headers=headers, params=params, data=data
        )

        # Log entire request and entire response
        self.http_log.write(f"{response.request.method} {response.request.url}\n")
        self.http_log.write(f"{response.request.headers}\n")
        self.http_log.write(f"{response.request.body}\n")
        self.http_log.write(f"\n {response.status_code}\n")
        self.http_log.write(f"{response.headers}\n")
        self.http_log.write(f"{response.text}\n")
        self.http_log.write(f"\n\n")

        return response
