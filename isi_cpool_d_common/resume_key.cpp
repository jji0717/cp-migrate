#include <stdlib.h>
#include <stdbool.h>
#include <iostream>
#include <sstream>

#include "resume_key.h"

resume_key::resume_key(void)
{
	resume_key_.clear();
}

/**
 * Check whether resume key is empty
 * @return	true if empty; otherwise false
 */
bool
resume_key::empty(void)
{
	return resume_key_.empty();
}

/**
 * Clear resume key to empty
 * @return	None
 */
void
resume_key::clear(void)
{
	return resume_key_.clear();
}

/**
 * Decode resume key into a btree_key_t structure
 * @return	btree_key_t
 */
btree_key_t
resume_key::decode(void)
{
	std::string token1;
	std::string token2;
	btree_key_t btkey;

	std::istringstream is(resume_key_);
	std::getline(is, token1, '.');
	btkey.keys[0] = strtoull(token1.c_str(), 0, 16);
	std::getline(is, token2);
	btkey.keys[1] = strtoull(token2.c_str(), 0, 16);

	return btkey;
}

/**
 * Encode a btree_key_t into a resume key
 * @param[in]	btkey  btree_key_t to be encoded
 * @return	None
 */
void
resume_key::encode(btree_key_t &btkey)
{
	std::stringstream ss;

	ss.width(16);
	ss.fill('0');

	ss << std::hex << btkey.keys[0];
	ss << ".";
	ss << std::hex << btkey.keys[1];
	resume_key_ = ss.str();
}

/**
 * Set resume key from a string
 * @param[in] resume_key_str
 */
void
resume_key::from_string(std::string resume_key_str)
{
	resume_key_ = resume_key_str;
}

/**
 * Return resume key as a string
 * @return	string
 */
std::string
resume_key::to_string(void)
{
	return resume_key_;
}
