#pragma once

/**
 * checkpoint interface definition
 */
class ckpt_intf
{
public:
	ckpt_intf() {}

	virtual ~ckpt_intf() {}

	/**
	 * read out checkpoint data starting at offset
	 * @param data    [out]:    data pointer
	 * @param size    [in/out]: size of the data
	 * @param offset  [in]:     checkpoint offset
	 */
	virtual bool get(void *&data, size_t &size, off_t offset) = 0;

	/**
	 * write in checkpoint data starting at offset
	 * @param data    [out]: data pointer
	 * @param size    [in]:  size of the data
	 * @param offset  [in]:  checkpoint offset
	 */
	virtual bool set(void *data, size_t size, off_t offset) = 0;

	/**
	 * remove the checkpoint data
	 */
	virtual void remove() = 0;
};

/**
 * checkpoint temp file implementation
 */
class file_ckpt : public ckpt_intf
{
public:
	file_ckpt(ifs_lin_t lin, ifs_snapid_t snapid);
	explicit file_ckpt(const std::string &fname);

	~file_ckpt();

	/** see ckpt_intf::get(...) **/
	bool get(void *&data, size_t &size, off_t offset);

	/** see ckpt_intf::set(...) **/
	bool set(void *data, size_t size, off_t offset);

	/** see ckpt_intf::remove **/
	void remove();

private:
	/**
	 * open temp file for read/write
	 */
	void open_if_not(struct isi_error **error_out);

private:
	int ckpt_fd_;

	// name of the checkpoint file
	std::string file_name_;
};
