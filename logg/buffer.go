package logg

import (
	"bytes"
)

type csvbuffer struct {
	bytes.Buffer
}

func (b *csvbuffer) Write(msgs ...string) {
	tmp := bytes.Buffer{}
	for _, msg := range msgs {
		quote := false
		tmp.Reset()

		for _, r := range msg {
			switch r {
			case '\r':
				continue
			case ',', '\n':
				tmp.WriteRune(r)
				quote = true
			case '"':
				tmp.WriteString("\"\"")
				quote = true
			default:
				tmp.WriteRune(r)
			}
		}

		if quote {
			b.Buffer.WriteRune('"')
		}
		b.Buffer.Write(tmp.Bytes())
		if quote {
			b.Buffer.WriteRune('"')
		}
		b.Buffer.WriteRune(',')
	}
}

func (b *csvbuffer) NewLine() {
	if b.Len() > 0 && b.Bytes()[b.Len()-1] == ',' {
		b.Truncate(b.Len() - 1)
	}
	b.WriteRune('\n')
}
