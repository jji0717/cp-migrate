#!/usr/bin/env python

from optparse import OptionParser
import re

# Quick-and-dirty Python script to read enum values from a C header file
# and use them to create a source file for formatting those enum values.
#
# Looks for start_enum_pattern, and after finding it, collects each enum value
# that matches enum_val_pattern.  Stops collecting after matching
# final_enum_val_pattern.  Complains if 0 enum values were found.

parser = OptionParser()
parser.add_option("-b", "--base", dest="base",
        help="base directory, defaults to '.'")
(options, args) = parser.parse_args()

base = options.base or '.'

in_enum = False
enum_values_found = []
start_enum_pattern = re.compile('\s*enum\s*cbm_error_type_values\s*{')
enum_val_pattern = re.compile('\s(\w+)\s*=?\s*\d*,')
final_enum_val_pattern = re.compile('\s*MAX_CBM_ERROR_TYPE')

print "Parsing",base + "/isi_cbm_error.h"
with open(base + "/isi_cbm_error.h", 'r') as header_file:
    for line in header_file.readlines():
        if not in_enum:
            m = start_enum_pattern.match(line)
            if m:
                in_enum = True
        else:
            m = enum_val_pattern.match(line)
            if m:
                enum_values_found.append(m.group(1))
            else:
                m = final_enum_val_pattern.match(line)
                if m:
                    in_enum = False

print "Found",len(enum_values_found),"enum values"
if len(enum_values_found) == 0:
    raise Exception("no enum values found")

# With enum_values_found populated, generate the source file.
header = '''
/*
 * This file is auto-generated by isi_cbm_error_gen.py.  Do NOT edit this
 * file directly!
 */

#include "isi_cbm_error.h"

MAKE_ENUM_FMT(cbm_error_type, enum cbm_error_type_values,
'''

footer = ");\n"

print "Creating",base + "/isi_cbm_error_enum_fmt.c"
with open(base + "/isi_cbm_error_enum_fmt.c", 'w') as source_file:
    source_file.write(header)
    for enum_value in enum_values_found:
        source_file.write('\tENUM_VAL(' + enum_value + '),\n')
    source_file.write(footer)
