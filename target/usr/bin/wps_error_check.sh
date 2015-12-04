#!/bin/bash

if [ $# != "1" ]; then 
	echo "Usage: $0 exec_path" > /dev/stderr
fi

exec_path=$1

# report_error error_identify error_message
function report_error()
{
	text="WPS Office for Linux\n======================\n\tA fatal error has occured!\n\n"$2"\n\n\tFor assistance click\n\t\t     <a href=\"http://community.wps.cn/wiki/$1\">HERE</a>"
	zenity --error --text="$text"
}

if readelf -h "$exec_path" | grep ELF32 > /dev/null && [ `uname -m` != "mips64" ]; then
	[ -f "/lib/ld-linux.so.2" ] || report_error system_no_32bit_support "  Your system does not appear to\n        support 32bit applications."
fi
if ldd "$exec_path" 2>&1 1>/dev/null | grep -F "libstdc++"; then
	report_error "libstdc++_too_old" "    Your libstdc++ is too old."
fi
