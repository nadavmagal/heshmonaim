import asyncio
import os
import toml
import sys
from google_sheets_reader_writer import GoogleSheetReaderWriter
from fizikal_api import FizikalAPI
import logging
import datetime
import pandas as pd
import datetime

FULL_CLASS = "השיעור התמלא"
CLASS_NOT_OPEN = "הרשמה לשיעור תיפתח ביום"
REGISTER_TOKEN = "v"
REMOVAL_TOKEN = "x"
DEFAULT_REGISTRATION_ID = -1
RELEVANT_COLS = [
    "id",
    "dateRequest",
    "description",
    "startTime",
    "endTime",
    "registered",
    "registrationId",
]

hours2seconds = lambda x: x * 60 * 60


def get_second_dif(date: str, time: str, subtract_day: bool = False):
    y, mon, d = date.split("-")
    h, m = time.split(":")
    target_date = datetime.datetime(
        int(y), month=int(mon), day=int(d), hour=int(h), minute=int(m)
    )
    if subtract_day:
        target_date = target_date - datetime.timedelta(days=1)
    return (target_date - datetime.datetime.now()).total_seconds()


class FizikalManager:
    def __init__(self, config_file: str = "config.toml", mock: bool = False):
        self.mock_http = mock
        self.classes = pd.DataFrame()
        self.tasks = dict()
        self.cancel_tasks = dict()
        self.__init_config(config_file)
        self._init_logging()
        self._init_api()
        self._init_google_sheets()

    def __init_config(self, config_file: str = "config.toml"):
        if not config_file:
            print("No config file specified")
            exit(1)
        elif not config_file.endswith(".toml"):
            print("Config file must be a .toml file")
            exit(1)
        elif not os.path.exists(config_file):
            print("Config file does not exist")
            exit(1)
        self.config = toml.load(config_file)

        self.persistent_storage = self.config.get("persistent_storage", "./persist")
        if not os.path.exists(self.persistent_storage):
            os.makedirs(self.persistent_storage, exist_ok=True)

        self.google_sheets_config = self.config.get("google_sheets", {})
        self.fizikal_config = self.config.get("fizikal", {})
        self.phone_number = self.fizikal_config.get("phone_number", None)
        if not self.phone_number:
            print("Phone number not specified in config file")
            exit(1)

    def _init_google_sheets(self):
        for key in self.google_sheets_config:
            if not self.google_sheets_config[key]:
                print(f"{key} not specified in google_sheets config")
                exit(1)
        self.google_sheet_rw = GoogleSheetReaderWriter(
            self.google_sheets_config["spreadsheet_name"],
            self.google_sheets_config["service_account_key"],
            self.google_sheets_config["sheet_name"],
        )
        logging.log(logging.INFO, "Google Sheets initialized")

    def _init_api(self):
        self.api = FizikalAPI(
            fizikal_config=self.fizikal_config,
            persistent_storage=self.persistent_storage,
            mock=self.mock_http,
        )
        logging.log(logging.INFO, "API initialized")

    def _init_logging(self):
        log_dir = os.path.join(self.persistent_storage, "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_file = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"
        log_path = os.path.join(log_dir, log_file)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
        )
        logging.log(logging.INFO, "Logging initialized")

    async def periodic_check_gsheets_for_registration_requests(
        self, interval: int = 60
    ):
        interval = self.google_sheets_config.get("polling_interval_minutes", 60)
        while True:
            logging.log(
                logging.INFO, "Checking Google Sheets for registration requests"
            )
            self.classes = self.google_sheet_rw.read_cells()
            await asyncio.sleep(hours2seconds(interval))

    async def periodic_get_classes(self):
        """
            Expected output:
            [
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
            ...
            ]
        """
        interval = self.fizikal_config.get("get_classes_every_x_hours", 60)
        while True:
            self.update_classes_from_sheet()
            logging.log(logging.INFO, "Updated classes from sheets")
            logging.log(logging.INFO, "Getting classes")
            new_classes = pd.DataFrame()
            try:
                for i in range(7):  # get next week's classes
                    classes = self.api.get_classes(i)
                    new_classes = pd.concat(
                        [new_classes, pd.DataFrame(classes)], ignore_index=True
                    )
                # new_classes["registered"] = REMOVAL_TOKEN
                # new_classes["registrationId"] = DEFAULT_REGISTRATION_ID
                self.merge_classes(new_classes)
                self.classes["as_date"] = self.classes.apply(
                    lambda y: datetime.date(
                        *[int(d) for d in y["dateRequest"].split("-")]
                    ),
                    axis=1,
                )
                self.classes.loc[self.classes['registered'].isna(), 'registered'] = REMOVAL_TOKEN
                self.classes.loc[self.classes['registrationId'].isna(), 'registrationId'] = DEFAULT_REGISTRATION_ID
                self.write_classes_to_google_sheets()
                self.beutify_google_sheets()
                self.classes = self.classes.loc[
                    self.classes.as_date >= datetime.date.today()
                ]
            except Exception as e:
                logging.log(logging.ERROR, f"Failed to get classes. Error: {e}")

            await asyncio.sleep(hours2seconds(interval))
    
    def merge_classes(self, new_classes):
        """
        Left merges classes
        """
        self.classes = pd.merge(new_classes,
                                self.classes,
                                how="left",
                                on=[r for r in RELEVANT_COLS if 'regis' not in r]
                                )
        
    
    def get_class_ids_registrations_dates(self):
        """
        gets all classes in which we are either registered or want to register (or both, but we'll ignore this)
        """
        if "registered" not in self.classes.columns:
            return [], [], []
        relevant_classes = self.classes.loc[
            (self.classes.registered == REGISTER_TOKEN)
            | (self.classes.registrationId > DEFAULT_REGISTRATION_ID),
            :,
        ]
        return (
            relevant_classes.id.values,
            relevant_classes.registrationId.values,
            relevant_classes.dateRequest.values,
        )

    async def periodic_register_classes(self):
        """
        Expected output:
        [
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
        ...
        ]
        """
        interval = self.fizikal_config.get("register_classes_every_x_hours", 60)
        await self.wait_for_classes()
        while True:
            logging.log(logging.INFO, "Registering Classes")
            self.update_classes_from_sheet()
            (
                classids,
                registration_ids,
                classdates,
            ) = self.get_class_ids_registrations_dates()

            for classid, classdate, reg_id in zip(
                classids, classdates, registration_ids
            ):
                if self.is_class_token(classid, REMOVAL_TOKEN) or (
                    reg_id > DEFAULT_REGISTRATION_ID
                ) or (classid, classdate) in self.tasks:  # already registered or task exists
                    continue
                try:
                    task = asyncio.create_task(self.register_class(classid, classdate))
                    self.tasks[(classid, classdate)] = task
                except Exception as e:
                    logging.log(
                        logging.ERROR, f"Failed to register to class. Error: {e}"
                    )
                    continue

            # if len(classids) > 0:
            #     self.write_classes_to_google_sheets()
            await asyncio.sleep(hours2seconds(interval))
    
    def is_class_token(self, classid, token):
        return (
                    self.classes.loc[self.classes.id == classid, "registered"]
                    == token
                ).all()

    async def register_class(self, classid, classdate):
        """
        calculates the start time of the class.
        Starts a timer up to the start of the registration
        """
        # get class starting time
        row = self.classes.loc[
            (self.classes.id == classid) & (self.classes.dateRequest == classdate), :
        ]
        classname = row.description
        logging.log(
            logging.INFO, f"Created task: register to {classname} at {classdate}"
        )
        time = row["startTime"].values[0]

        y, mon, d = classdate.split("-")
        h, m = time.split(":")
        end_time = datetime.datetime(
            int(y), month=int(mon), day=int(d), hour=int(h), minute=int(m)
        )

        # # subtract since we want to register a day in advance
        registration_date = end_time - datetime.timedelta(days=1)

        delay_seconds = (
            registration_date - datetime.datetime.now()
        ).total_seconds() - 0.005
        # Use asyncio.sleep to wait until the start time
        await asyncio.sleep(max(0, delay_seconds))

        while registration_date > datetime.datetime.now():
            pass
        while (classid, classdate) in self.tasks:
            try:
                classloc = self.classes.loc[self.classes.id == classid, RELEVANT_COLS]
                logging.log(logging.INFO, f"Registering Class\n{classloc}")
                resp = self.api.register_class(classid, classdate)
                self.classes.loc[
                    self.classes.id == classid, "registered"
                ] = REGISTER_TOKEN
                self.classes.loc[self.classes.id == classid, "registrationId"] = resp[
                    "registrationId"
                ]
                # update with the registration id
                self.google_sheet_rw.update_row(
                    row=self.classes.loc[self.classes.id == classid, RELEVANT_COLS],
                    sheet_name=classdate,
                )
                logging.log(logging.INFO, f"Successfully registered to class!")
                self.cancel_tasks[(classid, classdate)] = self.tasks[
                    (classid, classdate)
                ]
                del self.tasks[(classid, classdate)]
                break
            except Exception as e:  # Failed to registrate
                if FULL_CLASS in e.args[0]:
                    logging.log(
                        logging.ERROR, f"Failed to register class. Class is full"
                    )
                    break
                elif CLASS_NOT_OPEN in e.args[0]:
                    # logging.log(logging.ERROR, f"Failed to register class. Class is still not open")
                    await asyncio.sleep(0.5)

    async def wait_for_classes(self):
        while self.classes.empty:
            await asyncio.sleep(20)

    async def periodic_remove_classes(self):

        await self.wait_for_classes()
        while True:
            logging.log(logging.INFO, "Removing Classes")
            """
            Expected output:
            [
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
            ...
            ]
            """
            self.update_classes_from_sheet()
            (
                classids,
                registration_ids,
                rel_dates,
            ) = self.get_class_ids_registrations_dates()

            for classid, reg_id, classdate in zip(
                classids, registration_ids, rel_dates
            ):
                # marked as 'dont register' but has a registration id
                clause = self.is_class_token(classid, REMOVAL_TOKEN) and (reg_id > DEFAULT_REGISTRATION_ID)
                if clause:
                    try:
                        resp = self.api.remove_class(reg_id)
                        classname = self.classes.loc[
                            self.classes.registrationId == reg_id
                        ].description
                        self.classes.loc[
                            self.classes.id == classid, "registered"
                        ] = REMOVAL_TOKEN
                        self.classes.loc[
                            self.classes.id == classid, "registrationId"
                        ] = DEFAULT_REGISTRATION_ID
                        logging.log(
                            logging.INFO, f"Successfully removed class {classname}"
                        )
                    except Exception as e:
                        logging.log(
                            logging.ERROR, f"Failed to remove class. Error: {e}"
                        )
                        continue
                    self.google_sheet_rw.update_row(
                        row=self.classes.loc[self.classes.id == classid, RELEVANT_COLS],
                        sheet_name=classdate,
                    )
                if ((classid, classdate) in self.tasks) and self.is_class_token(classid, REMOVAL_TOKEN):
                    self.tasks[(classid, classdate)].cancel()
                    del self.tasks[(classid, classdate)]
            
            sleeptime = self.get_class_removal_sleeptime()
            
            await asyncio.sleep(sleeptime)

    def get_class_removal_sleeptime(self):
        interval = self.fizikal_config.get("remove_classes_every_x_hours", 60)
        sleeptime = hours2seconds(interval)
        next_upcoming_class = self.classes.loc[
            self.classes.registrationId > DEFAULT_REGISTRATION_ID
        ]
        next_upcoming_registration = self.classes.loc[
            (self.classes.registrationId == DEFAULT_REGISTRATION_ID)
            & (self.classes.registered == REGISTER_TOKEN)
        ]
        buffer_time_before_upcoming_class = -1
        if (
            not next_upcoming_class.empty
        ):  # sleep the minimum between the defined interval and 1 hour 3 hours before the next upcoming class
            next_upcoming_class = next_upcoming_class.iloc[0]
            class_date = next_upcoming_class.dateRequest
            class_time = next_upcoming_class.startTime
            buffer_time_before_upcoming_class = get_second_dif(
                class_date, class_time
            ) - hours2seconds(3)
        elif (
            not next_upcoming_registration.empty
        ):  # sleeping up until 5 seconds before next registration time
            next_upcoming_registration = next_upcoming_registration.iloc[0]
            class_date = next_upcoming_registration.dateRequest
            class_time = next_upcoming_registration.startTime
            buffer_time_before_upcoming_class = (
                get_second_dif(class_date, class_time, subtract_day=True) - 5 * 60
            )  # minus 5 minutes

        if buffer_time_before_upcoming_class < 0:
            pass
        else:
            sleeptime = min(buffer_time_before_upcoming_class, sleeptime)

        return sleeptime

    def update_classes_from_sheet(self):
        sheet_content = self.google_sheet_rw.read_cells()
        if len(sheet_content) > 0:
            self.classes = sheet_content
        self.classes["as_date"] = self.classes.apply(
                    lambda y: datetime.date(
                        *[int(d) for d in y["dateRequest"].split("-")]
                    ),
                    axis=1,
                )

    def write_classes_to_google_sheets(self, classes=None):
        if classes is None:
            classes = self.classes
        today = datetime.date.today()
        # write only dates in the next week
        classes = classes.loc[classes.as_date >= today]
        classes = classes.loc[classes.as_date <= (today + datetime.timedelta(days=7))]
        for date, datedf in classes.groupby("dateRequest"):
            self.google_sheet_rw.write_cells(datedf[RELEVANT_COLS], date)

    def beutify_google_sheets(self):
        for i in range(1, 7):
            x_days_ago = (datetime.date.today() - datetime.timedelta(days=i)).strftime(
                "%Y-%m-%d"
            )
            try:  # delete worksheets from past days
                self.google_sheet_rw.delete_worksheet(x_days_ago)
            except:
                pass

    async def periodic_remove_tasks(self):
        while True:
            tasks_to_dlt = list(self.cancel_tasks.keys())
            for k in tasks_to_dlt:
                self.cancel_tasks[k].cancel()
                del self.cancel_tasks[k]
            await asyncio.sleep(hours2seconds(0.5))  # sleep half an hour

    def start(self):
        self.loop = asyncio.get_event_loop()
        logging.log(logging.INFO, "Starting periodic tasks")
        self.loop.create_task(self.periodic_get_classes())
        self.loop.create_task(self.periodic_register_classes())
        self.loop.create_task(self.periodic_remove_classes())
        self.loop.create_task(self.periodic_remove_tasks())
        self.loop.run_forever()
        self.loop.close()

    def start_as_flask_server(self):
        pass

def restart_cache():
    exists = False
    name = ''
    for file in os.listdir('persist'):
        if 'fizikal_api' in file:
            name = file
            exists = True
    if exists:
        date = '/'.join(name.split('_')[-3:])
        if  (datetime.datetime.now() - datetime.datetime.strptime(date, "%m/%d/%Y")).days > 0:
            os.system(f"rm -f persist/{name}")


if __name__ == "__main__":
    # if len(sys.argv) < 2:
    #     print("Usage: python3 fizikal_manager.py <config.toml>")
    #     exit(1)
    pd.options.mode.chained_assignment = None 
    restart_cache()
    manager = FizikalManager(config_file="config.toml", mock=False)
    manager.start()
