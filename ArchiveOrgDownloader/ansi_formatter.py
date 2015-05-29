import datetime


class AnsiColors(object):
    HEADER = '\033[95m'
    OKCYAN = '\033[36m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    WHITEONBLUE = '\033[37;44m'
    WHITE = '\033[37m'
    DEBUG = WHITEONBLUE

    def disable(self):
        self.HEADER = ''
        self.OKCYAN = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.WARNING = ''
        self.FAIL = ''
        self.ENDC = ''
        self.WHITEONBLUE = ''
        self.WHITE = ''
        self.DEBUG = ''


class AnsiColorsFormater(object):
    OK_COLORS = ["CYAN", "BLUE", "GREEN"]

    def __init__(self, ok_color="GREEN", inform_type=False, timestamp=False):
        self.restore_colors()
        self.check_ok_colors(ok_color)
        self.ok_color = ok_color.upper()
        self.decorators = []
        if inform_type:
            self.enable_type()
        if timestamp:
            self.enable_timestamp()
        self.message_timestamp = "%Y-%m-%d %H:%M:%S"


    def success_message(self, *data, ok_alternative_color=None):
        if ok_alternative_color:
            ok_alternative_color = ok_alternative_color.upper()
            self.check_ok_colors(ok_alternative_color)
        color = ok_alternative_color or self.ok_color
        return self._print(getattr(self.colorer, "OK{0}".format(color)), '', *data)

    def info_message(self, *data):
        return self._print(self.colorer.OKBLUE, '', *data)

    def warning_message(self, *data):
        return self._print(self.colorer.WARNING, '', *data)

    def debug_message(self, *data):
        return self._print(self.colorer.DEBUG, '', *data)

    def error_message(self, *data):
        return self._print(self.colorer.FAIL, '', *data)

    def custom_message(self, initial_color="WHITE", end='', *data):
        try:
            initial_delimiter = getattr(self.colorer, initial_color)
        except Exception:
            raise Exception('{0} is not a valir initial color delimiter'.format(initial_color))
        return self._print(initial_delimiter, end, *data)

    def _print(self, initial, end='', *data):
        import inspect

        initial = self.decorate_initial(initial, inspect.stack()[1][3])
        new_data = (initial,) + tuple(data) + (end or self.colorer.ENDC,)
        return print(*new_data)

    def decorate_initial(self, initial, caller):
        args = {"message_timestamp": self.message_timestamp, "caller": caller}

        for decorator in self.decorators:
            initial = decorator.decorate(initial, **args)

        if len(self.decorators) > 0:
            initial += ":"
        return initial

    def check_ok_colors(self, color):
        if not color in self.OK_COLORS:
            raise Exception('{0} is not from one of the valid colors'.format(color))

    def enable_type(self):
        self.decorators.append(MessageTypeFormatter())
        return self

    def disable_type(self):
        self.remove_decorator(MessageTypeFormatter)
        return self

    def disable_timestamp(self):
        self.remove_decorator(TimeFormatter)
        return self

    def remove_decorator(self, type_decorator):
        for decorator in self.decorators:
            if isinstance(decorator, type_decorator):
                self.decorators.remove(decorator)

    def enable_timestamp(self):
        self.decorators.append(TimeFormatter())
        return self

    def disable_colors(self):
        self.colorer.disable()
        return self

    def restore_colors(self):
        self.colorer = AnsiColors()
        return self


class FormaterDecorator(object):
    def decorate(self, initial, *args, **kwargs):
        raise NotImplementedError


class TimeFormatter(FormaterDecorator):
    def decorate(self, initial, *args, **kwargs):
        initial += '[{0}]'.format(datetime.datetime.now().strftime(kwargs["message_timestamp"]))
        return initial


class MessageTypeFormatter(FormaterDecorator):
    def decorate(self, initial, *args, **kwargs):
        initial += '[{0}]'.format(kwargs["caller"].replace('_message', '').title())
        return initial


if __name__ == '__main__':
    formater = AnsiColorsFormater()
    formater.success_message('Hola esto funco bien')
    formater.warning_message('Esto es un warning')
    formater.enable_timestamp()
    formater.success_message('Hola esto funco bien')
    formater.enable_type()
    formater.success_message('Hola esto funco bien')
    formater.disable_type()
    formater.success_message('Hola esto funco bien')
    formater.debug_message('Some debug message')