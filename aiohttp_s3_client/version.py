author_info = (("Dmitry Orlov", "me@mosquito.su"),)

package_info = (
    "The simple module for putting and getting object from Amazon S3 "
    "compatible endpoints"
)

package_license = "Apache Software License"

team_email = "me@mosquito.su"

version_info = (0, 4, 0)

__author__ = ", ".join("{} <{}>".format(*info) for info in author_info)
__version__ = ".".join(map(str, version_info))
