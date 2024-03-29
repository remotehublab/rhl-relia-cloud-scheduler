from flask import current_app

class ErrorKeys:

    uniqueIdentifier = "uniqueIdentifier"
    author = "author"
    errorMessage = "errorMessage"
    errorTime = "errorTime"

    @staticmethod
    def errors() -> str:
        return f"{ErrorKeys.base_key()}:relia:scheduler:errors"

    @staticmethod
    def identifier(identifier) -> str:
        return f"{ErrorKeys.base_key()}:relia:scheduler:errors:{identifier}"

    @staticmethod
    def base_key():
        return current_app.config.get('BASE_KEY') or 'base'

class TaskKeys:

    uniqueIdentifier = "uniqueIdentifier"
    author = "author"
    transmitterFile = "transmitterFile"
    receiverFile = "receiverFile"
    transmitterFilename = "transmitterFilename"
    receiverFilename = "receiverFilename"
    sessionId = "sessionId"
    startedTime = "startedTime"
    priority = "priority"
    transmitterAssigned = "transmitterAssigned"
    receiverAssigned = "receiverAssigned"
    deviceAssigned = "deviceAssigned"
    transmitterProcessingStart = "transmitterProcessingStart"
    receiverProcessingStart = "receiverProcessingStart"
    status = "status"
    errorMessage = "errorMessage"
    errorTime = "errorTime"
    localTimeRemaining = "localTimeRemaining"
    inactiveSince = "inactiveSince"
    transmitterFiletype = "transmitterFiletype"
    receiverFiletype = "receiverFiletype"

    class Status:
        completed = 'completed'
        queued = 'queued'
        deleted = 'deleted'
        error = 'error'
        receiver_still_processing = 'receiver-still-processing'
        transmitter_still_processing = 'transmitter-still-processing'
        receiver_assigned = 'receiver-assigned'
        fully_assigned = 'fully-assigned'

    @staticmethod
    def tasks() -> str:
        return f"{TaskKeys.base_key()}:relia:scheduler:tasks"

    @staticmethod
    def identifier(identifier) -> str:
        return f"{TaskKeys.base_key()}:relia:scheduler:tasks:{identifier}"

    @staticmethod
    def base_key():
        return current_app.config.get('BASE_KEY') or 'base'

    @staticmethod
    def priority_queue(priority: int) -> str:
        return f"{TaskKeys.base_key()}:relia:scheduler:tasks:queues:{priority}"

    @staticmethod
    def priorities() -> str:
        return f"{TaskKeys.base_key()}:relia:scheduler:priorities"

class DeviceKeys:
    def __init__(self, device_id):
        self.base_key = current_app.config.get('BASE_KEY') or 'base'
        self.device_id = device_id

    def devices(self):
        return f"{self.base_key}:relia:scheduler:devices:{self.device_id}"
    
    def last_check(self):
        return f"{self.base_key}:relia:scheduler:devices:{self.device_id}:last_check"

    @staticmethod
    def device_assignment(device_id):
        return f'{DeviceKeys.base_key()}:relia:scheduler:devices:{device_id}:assigned_task'

    @staticmethod
    def credentials():
        return f"{DeviceKeys.base_key()}:relia:scheduler:device-credentials"

    @staticmethod
    def base_key():
        return current_app.config.get('BASE_KEY') or 'base'

