def write_log(
    svrty: str,
    err_msg: str = "",
    family: str = "",
    excp: Exception = None,
) -> None:
    from traceback import format_exception
    from sys import stderr, _getframe
    from os import getpid
    from datetime import datetime
    from json import dumps

    log_details = {
        "svrty": svrty.upper(),
        "family": family,
        "time_stamp": str(datetime.now()),
        "pid": getpid(),
        "err_msg": err_msg
    }

    excp_msg = None
    filename = None
    lineno = None
    try:
        if excp:
            excp_msg = "".join(
                format_exception(excp.__class__, excp, excp.__traceback__)
            )

        frameinfo = _getframe().f_back
        if frameinfo:
            filename = frameinfo.f_code.co_filename
            lineno = frameinfo.f_lineno
            frameinfo = frameinfo.f_back
            if frameinfo:
                filename = frameinfo.f_code.co_filename
                lineno = frameinfo.f_lineno

        log_details["filename"] = filename
        log_details["lineno"] = lineno
        log_details["Exception"] = excp_msg
        log_file = "error.log"
        if log_file is None:
            stderr.write(dumps(log_details))
        else:
            with open(log_file, "a") as f:
                f.write(f"{dumps(log_details)}\n")

    except Exception:
        stderr.write(dumps(log_details))

class DownloadException(Exception):
    def __init__(
        self, 
        err_msg: str = "",
        family: str = "issue_download",
        excp: Exception = None,
    ):
        self.excp = excp
        self.family = family
        self.err_msg = err_msg
        write_log("issue", err_msg, family, excp)

    def __str__(self) -> str:
        if self.excp:
            return f"{self.family} {type(self.excp).__name__} {self.err_msg}"
        else:
            return f"{self.family}: {self.err_msg}"

class EmailException(Exception):
    def __init__(
        self, 
        err_msg: str = "",
        family: str = "issue_email",
        excp: Exception = None,
    ):
        self.excp = excp
        self.family = family
        self.err_msg = err_msg
        write_log("issue", err_msg, family, excp)

    def __str__(self) -> str:
        if self.excp:
            return f"{self.family} {type(self.excp).__name__} {self.err_msg}"
        else:
            return f"{self.family}: {self.err_msg}"


class YamlException(Exception):
    def __init__(
        self,
        err_msg: str = "",
        family: str = "issue_yaml",
        excp: Exception = None,
    ):
        self.excp = excp
        self.family = family
        self.err_msg = err_msg

        write_log("crach", err_msg, family, excp)

    def __str__(self) -> str:
        if self.excp:
            return f"{self.family} {type(self.excp).__name__} {self.err_msg}"
        else:
            return f"{self.family}: {self.err_msg}"
        
class FatalException(Exception):
    def __init__(
        self, 
        err_msg: str = "",
        family: str = "issue_code",
        excp: Exception = None,
    ):
        self.excp = excp
        self.family = family
        self.err_msg = err_msg
        write_log("crash", err_msg, family, excp)

    def __str__(self) -> str:
        if self.excp:
            return f"{self.family} {type(self.excp).__name__} {self.err_msg}"
        else:
            return f"{self.family}: {self.err_msg}"