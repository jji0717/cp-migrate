#include <string>
#include <stdbool.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <err.h>
#include <stdlib.h>
#include <sysexits.h>

#include <isi_flexnet/isi_flexnet.h>
#include <isi_util/isi_printf.h>
#include <isi_util/util_adt.h>
#include <isi_inx_addr/inx_addr.h>

char const HARDENING_INFO_PATH [] = "/etc/ifs/hardening_info.txt";
int const IPV4 = 1;
int const IPV6 = 2;

bool test_is_hardened(void);

char *inx_addr_to_str(const struct inx_addr *addr);

char  *test_get_external_ip();

int test_is_valid_ip(char *ip_addr);

int test_make_url(std::string &test_url, const std::string protocol, int  port, const std::string path);
