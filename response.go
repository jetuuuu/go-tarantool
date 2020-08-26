package tarantool

import (
	"fmt"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Response struct {
	RequestId uint32
	Code      uint32
	Error     string // error message
	// Data contains deserialized data for untyped requests
	Data               []interface{}
	Meta               map[string]string
	SQLChangedRowCount uint64
	buf                smallBuf
}

func (resp *Response) smallInt(d *msgpack.Decoder) (i int, err error) {
	b, err := resp.buf.ReadByte()
	if err != nil {
		return
	}
	if b <= 127 {
		return int(b), nil
	}
	resp.buf.UnreadByte()
	return d.DecodeInt()
}

func (resp *Response) decodeHeader(d *msgpack.Decoder) (err error) {
	var l int
	d.Reset(&resp.buf)
	if l, err = d.DecodeMapLen(); err != nil {
		return
	}
	for ; l > 0; l-- {
		var cd int
		if cd, err = resp.smallInt(d); err != nil {
			return
		}
		switch cd {
		case KeySync:
			var rid uint64
			if rid, err = d.DecodeUint64(); err != nil {
				return
			}
			resp.RequestId = uint32(rid)
		case KeyCode:
			var rcode uint64
			if rcode, err = d.DecodeUint64(); err != nil {
				return
			}
			resp.Code = uint32(rcode)
		default:
			if err = d.Skip(); err != nil {
				return
			}
		}
	}
	return nil
}

func (resp *Response) decodeBody() error {
	resp.Meta = make(map[string]string)

	if resp.buf.Len() <= 2 {
		return nil
	}

	d := msgpack.NewDecoder(&resp.buf)

	l, err := d.DecodeMapLen()
	if err != nil {
		return err
	}

	for ; l > 0; l-- {
		cd, err := resp.smallInt(d)
		if err != nil {
			return err
		}

		switch cd {
		case KeyData:
			res, err := d.DecodeInterface()
			if err != nil {
				return err
			}

			data, ok := res.([]interface{})
			if !ok {
				return fmt.Errorf("result is not array: %v", res)
			}

			resp.Data = data
		case KeySQLInfo:
			i, err := d.DecodeMap()
			if err != nil {
				return err
			}

			info := i.(map[interface{}]interface{})
			resp.SQLChangedRowCount = info[uint64(0)].(uint64)
		case KeyError:
			resp.Error, err = d.DecodeString()
			if err != nil {
				return err
			}
		case KeyMetaData:
			meta, err := d.DecodeSlice()
			if err != nil {
				return err
			}

			for _, m := range meta {
				metaMap, ok := m.(map[interface{}]interface{})
				if ok {
					key, ok := metaMap[uint64(0)].(string)
					if ok {
						if value, ok := metaMap[uint64(1)].(string); ok {
							resp.Meta[key] = value
						}
					}
				}
			}
		default:
			fmt.Println("skip: ", cd)
			if err = d.Skip(); err != nil {
				return err
			}
		}
	}

	if resp.Code != OkCode {
		resp.Code &^= ErrorCodeBit

		return Error{resp.Code, resp.Error}
	}

	return nil
}

func (resp *Response) decodeBodyTyped(res interface{}) (err error) {
	if resp.buf.Len() > 0 {
		var l int
		d := msgpack.NewDecoder(&resp.buf)
		if l, err = d.DecodeMapLen(); err != nil {
			return err
		}
		for ; l > 0; l-- {
			var cd int
			if cd, err = resp.smallInt(d); err != nil {
				return err
			}
			switch cd {
			case KeyData:
				if err = d.Decode(res); err != nil {
					return err
				}
			case KeyError:
				if resp.Error, err = d.DecodeString(); err != nil {
					return err
				}
			default:
				if err = d.Skip(); err != nil {
					return err
				}
			}
		}
		if resp.Code != OkCode {
			resp.Code &^= ErrorCodeBit
			err = Error{resp.Code, resp.Error}
		}
	}
	return
}

// String implements Stringer interface
func (resp *Response) String() (str string) {
	if resp.Code == OkCode {
		return fmt.Sprintf("<%d OK %v>", resp.RequestId, resp.Data)
	}
	return fmt.Sprintf("<%d ERR 0x%x %s>", resp.RequestId, resp.Code, resp.Error)
}

// Tuples converts result of Eval and Call17 to same format
// as other actions returns (ie array of arrays).
func (resp *Response) Tuples() (res [][]interface{}) {
	res = make([][]interface{}, len(resp.Data))
	for i, t := range resp.Data {
		switch t := t.(type) {
		case []interface{}:
			res[i] = t
		default:
			res[i] = []interface{}{t}
		}
	}
	return res
}
