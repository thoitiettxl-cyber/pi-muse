# Third-party notices

## oh-my-pi

`src/xml-dialect.ts` adapts code and prompt text from oh-my-pi
(https://github.com/can1357/oh-my-pi, commit e19bb3c), files
`packages/ai/src/dialect/xml.md`, `prompt-template.md`, `coercion.ts` and `rendering.ts`:
the XML `<invoke>/<parameter>` format guide and rules, schema type collection /
string-only detection / JSON value decoding, and `<tool_response>` rendering.
The streaming in-band scanner was replaced by a whole-reply parser.

```
MIT License

Copyright (c) 2025 Mario Zechner
Copyright (c) 2025-2026 Can Bölük
Copyright (c) 2026 Stencil Labs, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## muse2api

`backend/pi_native.py` and `backend/muse2api-app.patch` are meant to be added to
muse2api (https://github.com/czg86389-hub/muse2api, MIT) and use its engine, store and
account pool. muse2api itself is not vendored here.
