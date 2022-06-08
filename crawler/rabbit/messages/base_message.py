class BaseMessage:
    def __init__(self):
        self._textual_errors = []

    @property
    def textual_errors_summary(self):
        error_count = len(self._textual_errors)

        if error_count == 0:
            errors_label = "No errors were"
        elif error_count == 1:
            errors_label = "1 error was"
        else:
            errors_label = f"{error_count} errors were"

        additional_text = " Only the first 5 are shown." if error_count > 5 else ""

        error_list = [f"{errors_label} reported during processing.{additional_text}"] + self._textual_errors[:5]

        return error_list

    def add_textual_error(self, description):
        self._textual_errors.append(description)
