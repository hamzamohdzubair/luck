# Luck - Random Learning Resource Picker

A CLI tool that opens random learning resources at random locations to spark curiosity and encourage serendipitous learning.

## Features

- 📚 **Multiple Resource Types**: PDFs, physical books, YouTube videos/playlists, courses, articles, and more
- 🏷️ **Tag-Based Filtering**: Organize resources with custom tags and filter by them
- 📖 **Structured Books**: Support for books with chapters/sections/subsections
- 🎯 **Random Selection**: Picks random resources and random locations within them
- 🎨 **Markdown Config**: Easy-to-edit configuration in clean markdown tables

## Installation

### From Source

```bash
git clone https://github.com/yourusername/luck
cd luck
cargo install --path .
```

### From crates.io (coming soon)

```bash
cargo install luck
```

## Quick Start

1. **Pick a random resource**:
   ```bash
   luck pick
   ```

2. **Edit your config**:
   ```bash
   luck config
   ```

3. **Filter by tags**:
   ```bash
   luck pick youtube    # Only YouTube resources
   luck pick book       # Only books
   luck pick physical   # Only physical books
   ```

## Configuration

Config file is located at `~/.config/luck/luck.md` (auto-created on first run).

### Supported Resource Types

The tool automatically detects resource types based on table structure:

#### 1. PDF Folders
Scans directories for PDFs and opens them at random pages.

```markdown
## book, pdf

| Path |
|------|
| ~/Documents/Books |
| ~/Google Drive/Learning |
```

#### 2. Physical Books
Books with total page count (for physical books, e-readers, etc.).

```markdown
## book, physical

| Title | Pages |
|-------|-------|
| Deep Work | 296 |
| The Pragmatic Programmer | 352 |
```

#### 3. Structured Books
Books organized by chapters and sections.

```markdown
## textbook, structured

| Title | Structure |
|-------|-----------|
| Algorithms 4th Edition | [10, 8, 12, 15] |
| CLRS | [[5, 3, 8], [10, 4], [6, 7, 9, 2]] |
```

- `[10, 8, 12, 15]` - 4 chapters with 10, 8, 12, and 15 sections respectively
- `[[5, 3], [10, 4]]` - Chapters with sections, each section having subsections

#### 4. YouTube Videos
Opens at random timestamps.

```markdown
## youtube, video

| Hint | URL |
|------|-----|
| Advanced Rust | <https://www.youtube.com/watch?v=dQw4w9WgXcQ> |
```

#### 5. YouTube Playlists
Picks random video from playlist, opens at random timestamp.

```markdown
## youtube, playlist

| Hint | URL |
|------|-----|
| MIT Algorithms | <https://www.youtube.com/playlist?list=PLxyz123> |
```

### Custom Tags

Section headers use comma-separated tags for filtering:

```markdown
## course, udemy, programming

| Course Name | Lectures |
|-------------|----------|
| Advanced TypeScript | 120 |
```

Filter with any tag:
- `luck pick course` - matches "course"
- `luck pick udemy` - matches "udemy"
- `luck pick prog` - matches "programming" (partial match)

## Examples

### Random from any resource
```bash
$ luck pick
📖 Open "Deep Work" to page 142
```

### Filter by YouTube
```bash
$ luck pick you
🎥 Selected video: Advanced Rust
🔗 https://www.youtube.com/watch?v=dQw4w9WgXcQ
```

### Filter by books only
```bash
$ luck pick book
📖 Open "Algorithms 4th Edition" Chapter 2, Section 5
```

### No matches
```bash
$ luck pick xyz
No sections found matching filter 'xyz'.
Available tags: book, pdf, youtube, video, course, udemy
```

## Commands

- `luck pick [FILTER]` - Pick a random resource (optionally filtered by tags)
- `luck config` - Open config file in `$EDITOR`
- `luck --help` - Show help

## How It Works

1. Parses your config file (`~/.config/luck/luck.md`)
2. Groups resources by section tags
3. Filters sections by your query (if provided)
4. Randomly picks a section
5. Randomly picks a resource from that section
6. Randomly picks a location within that resource

## Use Cases

- **Break reading monotony**: Jump to random chapters instead of reading linearly
- **Spaced repetition**: Randomly revisit different parts of learning materials
- **Procrastination buster**: Let luck decide what to study
- **Course sampling**: Quickly sample different lectures/chapters
- **Curiosity driver**: Serendipitous learning through random discovery

## Roadmap

- [x] Tag-based filtering
- [x] Structured book support
- [ ] PDF scanning and opening
- [ ] YouTube timestamp generation and browser opening
- [ ] YouTube playlist video fetching (via yt-dlp)
- [ ] Statistics tracking
- [ ] Daily random resource scheduling

## Contributing

Contributions welcome! Please open an issue or PR.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
