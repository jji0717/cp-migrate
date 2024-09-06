#pragma once

/**
 * Value of the tri-state boolean value
 */
enum isi_tri_bool_state {
	TB_FALSE = 0,
	TB_TRUE = 1,
	TB_UNKNOWN = 255,
};

/**
 * Three state boolean
 */
class isi_tri_bool {
public:
	isi_tri_bool(isi_tri_bool_state st) : state_(st) {}
	isi_tri_bool(bool st) : state_(st ? TB_TRUE : TB_FALSE) {}

	/**
	 * Only true is really true, false and unknown is false
	 */
	operator bool() const { return state_ == TB_TRUE; }

	/**
	 * check if the state is unknown
	 */
	bool unknown() const { return state_ == TB_UNKNOWN; }

	bool operator==(const isi_tri_bool &other) const
	{
		return state_ == other.state_;
	}

	bool operator!=(const isi_tri_bool &other) const
	{
		return state_ != other.state_;
	}

	bool operator!=(bool state) const
	{
		return state_ != state_;
	}

	bool operator==(isi_tri_bool_state state) const
	{
		return state_ == state;
	}

	bool operator!=(isi_tri_bool_state state) const
	{
		return (state_ != state);
	}

	/**
	 * assignment operators
	 */
	isi_tri_bool &operator=(bool st) {
		state_ = (st ? TB_TRUE : TB_FALSE);
		return *this;
	}

	isi_tri_bool &operator=(isi_tri_bool_state st) {
		state_ = st;
		return *this;
	}

	void pack(void **str) const {
		int pk_sz = get_pack_size();
		uint8_t temp = (uint8_t)state_;
		memcpy(*str, &temp, pk_sz);
		*((char **)str) += pk_sz;
	}

	void unpack(void **str) {
		int pk_sz = get_pack_size();
		uint8_t temp = 0;
		memcpy(&temp, *str, pk_sz);
		state_ = (isi_tri_bool_state)temp;
		*((char **)str) += pk_sz;
	}

	static int get_pack_size() {
		return sizeof(uint8_t);
	}

	const isi_tri_bool_state &get_state() const { return state_; }

	const char *c_str() const {
		const char *str = NULL;

		switch(state_) {
			case TB_TRUE:
				str = "YES";
				break;
			case TB_FALSE:
				str = "NO";
				break;
			default:
				str = "Unknown";
				break;
		}
		return str;
	}

private:
	isi_tri_bool_state state_;
};
