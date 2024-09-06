#ifndef __RESUME_KEY_H__
#define __RESUME_KEY_H__

#include <sys/types.h>
#include <string>
#include <ifs/ifs_types.h>

struct resume_key {
public:
	resume_key(void);  /* default empty resume key */

	~resume_key(void){};

	bool empty(void);

	void clear(void);

	/* decode resume_key into a btree_key_t structure */
	btree_key_t decode(void);

	/* encode a btree_key_t structure into a resume key */
	void encode(btree_key_t &btkey);

	/* set resume key from a string */
	void from_string(std::string resume_key_str);

	/* convert resume key into a string */
	std::string to_string(void);

private:
	std::string resume_key_;
};

#endif
