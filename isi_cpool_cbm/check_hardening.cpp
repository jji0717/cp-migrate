#include "check_hardening.h"

char *
test_get_external_ip()
{
    static struct flx_config *fcfg = NULL;
    struct isi_error *e = NULL;
    char* addr_str =NULL;
    fcfg = flx_config_load_local(NULL, &e);
    if (e) {
        errx(EX_SOFTWARE, "error reading flexnet config %s",
        isi_error_get_message(e));
        isi_error_free(e);
    }

    for (flx_alloc_method method = FLX_ALLOC_METHOD_STATIC; method < FLX_NUM_ALLOC_METHODS; ++method) {
        struct inx_addr_set *aset;
        int len;

        aset = flx_get_node_inx_addrs_by_type(fcfg, DEVID_LOCAL, method, LNI_ANY_IDX, IFFLAG_EXTERNAL, AF_UNSPEC);

        if ((len = inx_addr_set_size(aset)) > 0) {
        addr_str = inx_addr_to_str(&aset->addrs[0]);
        break;
        }
        free(aset);
    }
    flx_config_free(fcfg);
    return addr_str;
}

bool
test_is_hardened()
{
    if (access(HARDENING_INFO_PATH, F_OK ) == 0) {
        return true;
    }
    return false;
}

int
test_is_valid_ip(char *ip_addr)
{
	unsigned char buf[sizeof(struct in6_addr)];

	if (inet_pton(AF_INET, ip_addr, buf) == 1)
	    return IPV4;

	if (inet_pton(AF_INET6, ip_addr, buf) == 1)
	    return IPV6;

	return -1;
}

//Following function is copied from isi_migrate library.
//It converts IP address in string format.
char *
inx_addr_to_str(const struct inx_addr *addr) {
    struct fmt FMT_INIT_CLEAN(addr_fmt);
    char *addr_str = (char*)malloc(INET6_ADDRSTRLEN);

    fmt_print(&addr_fmt, "%{}", inx_addr_fmt(addr));
    snprintf(addr_str, INET6_ADDRSTRLEN, "%s", fmt_string(&addr_fmt));
    return addr_str;
}

int
test_make_url(std::string &test_url, const std::string protocol,
              int port = 0, const std::string path = "")
{
    char *node_ip = NULL;
    int  check_ip = -1;

    node_ip = test_get_external_ip();
    check_ip = test_is_valid_ip(node_ip);
    if (check_ip == -1) {
        free(node_ip);
        return -1;
    } else {
        test_url.assign(protocol);
        test_url.append("://");


        if (check_ip == IPV4) {
            test_url.append(node_ip);
        } else if (check_ip == IPV6) {
            test_url.append("[");
            test_url.append(node_ip);
            test_url.append("]");
        }
        if (port > 0 ) {
            test_url.append(":");
            test_url.append(std::to_string(port));
        }
        if (path.length() > 0) {
            test_url.append(path);
        }
        free(node_ip);
        return 0;
    }
}

