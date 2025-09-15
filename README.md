# mlbrecaps

mlbrecaps is a Python library for querying and retrieving highlight videos and play information from Major League Baseball (MLB) games. It provides a simple interface to access game recaps, top plays, and player highlights programmatically.

## Features

- Query highlight videos for specific MLB games
- Retrieve top plays for a given day, month, or year
- Get player-specific highlight clips
- Easily integrate with your own Python scripts

## Installation

You can install mlbrecaps directly from PyPI using pip:

```bash
pip install mlbrecaps
```

### Install from Source

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/mlbrecaps.git
   cd mlbrecaps
   ```

2. **Install dependencies with uv:**

   ```bash
   uv pip install -e .
   ```

   This will install the package in editable mode along with all required dependencies.

## Example Scripts

The `examples/` directory contains ready-to-run scripts:

- `examples/top_player_plays.py` — Get top plays for a player
- `examples/top_plays_of_month.py` — Get top plays for a month
- `examples/top_plays_of_year.py` — Get top plays for a year

Run an example with:

```bash
python examples/top_player_plays.py
```

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository and create your branch.
2. Make your changes and add tests if applicable.
3. Ensure code style and formatting are consistent.
4. Submit a pull request with a clear description of your changes.

## License

This project is open source and available under the MIT License.
