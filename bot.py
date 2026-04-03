#!/usr/bin/env python3
"""
Zaily Games Leaderboard Bot for Discord — Message History Edition
Monthly reset to prevent backlog accumulation
"""

import re, os, logging, asyncio, unicodedata
from datetime import datetime, timedelta, timezone, time as dt_time
from collections import defaultdict
from typing import Optional, Literal
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ═══════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

TOKEN      = os.getenv("DAILY_GAMES_BOT_TOKEN")
PREFIX     = "!zg "
RESET_DAY  = int(os.getenv("RESET_DAY", "1"))

log = logging.getLogger("zailygames")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")

MEDALS = ["🥇", "🥈", "🥉"]


# ═══════════════════════════════════════════════════════════════════
#  IN-MEMORY STORAGE (Monthly reset)
# ═══════════════════════════════════════════════════════════════════

class ResultsStore:
    """In-memory storage with monthly reset and crown tracking"""
    
    def __init__(self):
        self.results: dict[tuple, dict] = {}
        self.current_month: int = datetime.now(timezone.utc).month
        # Track crown reset dates per guild: {guild_id: datetime}
        self.crown_reset_dates: dict[str, datetime] = {}
        # Map display_name -> user_id for each guild to handle Wordle summaries
        self.name_to_id: dict[tuple[str, str], str] = {}
    
    def map_name_to_id(self, guild_id: str, name: str, user_id: str):
        """Map a display name to a user ID for consistent identification"""
        self.name_to_id[(guild_id, name)] = user_id
        # Also reconcile any existing Wordle entries with this name
        self._reconcile_wordle_entries(guild_id, name, user_id)
    
    def _reconcile_wordle_entries(self, guild_id: str, name: str, user_id: str):
        """Update existing Wordle entries to use the correct user_id"""
        updated = 0
        keys_to_update = []
        
        for key, r in self.results.items():
            if (r["guild_id"] == guild_id and 
                r["game"] == "Wordle" and 
                r["user_id"] == name and  # Currently stored with name as ID
                r["username"] == name):
                # Found a Wordle entry that needs updating
                keys_to_update.append(key)
        
        for old_key in keys_to_update:
            r = self.results[old_key]
            # Create new key with correct user_id
            new_key = (guild_id, user_id, r["game"], r["puzzle_date"])
            # Update the record
            r["user_id"] = user_id
            self.results[new_key] = r
            del self.results[old_key]
            updated += 1
        
        if updated > 0:
            log.info(f"Reconciled {updated} Wordle entries for {name} -> {user_id}")
    
    def get_user_id_from_name(self, guild_id: str, name: str) -> str:
        """Get user ID from display name, or return cleaned name if not mapped. 
        Handles Discord mentions like <@123456789> by extracting the ID."""
        # If it's already a mention format, extract the ID
        if name.startswith('<@') and name.endswith('>'):
            return name[2:-1]  # Remove <@ and >
        if name.startswith('<@!') and name.endswith('>'):
            return name[3:-1]  # Remove <@! and >
        
        # Otherwise look up in mapping
        return self.name_to_id.get((guild_id, name), name)
    
    def check_reset(self):
        """Reset storage if we've entered a new month"""
        now = datetime.now(timezone.utc)
        if now.month != self.current_month and now.day >= RESET_DAY:
            old_month = self.current_month
            self.results.clear()
            self.current_month = now.month
            log.info("🗑️  Monthly reset triggered: cleared %d results from month %d", 
                     len(self.results), old_month)
            return True
        return False
    
    def reset_crowns(self, guild_id: str):
        """Reset crown count for a guild - crowns will only count from today forward (not exact time)"""
        # Use today's date in Eastern Time (midnight), not exact timestamp
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today_start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        self.crown_reset_dates[guild_id] = today_start_et
        log.info(f"👑 Crowns reset for guild {guild_id} at {today_start_et} (all games from today count)")
        return today_start_et
    
    def get_crown_reset_date(self, guild_id: str) -> Optional[datetime]:
        """Get the crown reset date for a guild, or None if never reset"""
        return self.crown_reset_dates.get(guild_id)
    
    def save(self, guild_id: str, user_id: str, username: str, 
             game_result, date_str: str):
        """Save a result, overwriting any existing entry for same user/game/date"""
        key = (guild_id, user_id, game_result.game, date_str)
        self.results[key] = {
            "guild_id": guild_id,
            "user_id": user_id,
            "username": username,
            "game": game_result.game,
            "score": game_result.score,
            "max_score": game_result.max_score,
            "display": game_result.display,
            "puzzle_date": date_str,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
    
    def fetch(self, guild_id: str, *, date=None, start=None, end=None, 
              game=None, uid=None):
        """Fetch results matching criteria"""
        rows = []
        for key, r in self.results.items():
            if r["guild_id"] != guild_id:
                continue
            if date and r["puzzle_date"] != date:
                continue
            if start and r["puzzle_date"] < start:
                continue
            if end and r["puzzle_date"] > end:
                continue
            if game and r["game"].lower() != game.lower():
                continue
            if uid and r["user_id"] != uid:
                continue
            rows.append(r)
        meta_low = {p["name"].lower(): p["low"] for p in _PARSERS}
        rows.sort(key=lambda r: (
            r["puzzle_date"], 
            r["game"], 
            r["score"] if meta_low.get(r["game"].lower(), False) else -r["score"]
        ), reverse=True)
        return rows
    
    def get_all(self, guild_id: str):
        """Get all results for a guild"""
        return [r for r in self.results.values() if r["guild_id"] == guild_id]


store = ResultsStore()


# ═══════════════════════════════════════════════════════════════════
#  PARSER REGISTRY
# ═══════════════════════════════════════════════════════════════════

_PARSERS: list[dict] = []
_GAME_META: dict[str, dict] = {}


class GameResult:
    __slots__ = ("game", "score", "max_score", "display")
    def __init__(self, game, score, max_score, display):
        self.game      = game
        self.score     = score
        self.max_score = max_score
        self.display   = display


def game_parser(name: str, pattern: str, *,
                lower_is_better: bool = False, icon: str = "🎮"):
    compiled = re.compile(pattern, re.DOTALL | re.IGNORECASE)
    def deco(fn):
        entry = dict(name=name, pat=compiled, fn=fn, low=lower_is_better, icon=icon)
        _PARSERS.append(entry)
        _GAME_META[name.lower()] = entry
        return fn
    return deco


def parse_message(text: str) -> list[GameResult]:
    out: list[GameResult] = []
    for p in _PARSERS:
        m = p["pat"].search(text)
        if not m:
            continue
        ex = p["fn"](m)
        if ex is None:
            continue
        score, mx, disp = ex
        out.append(GameResult(p["name"], score, mx, disp))
    return out


def _match_game(query: str) -> Optional[str]:
    q = query.lower().strip()
    for p in _PARSERS:
        if p["name"].lower() == q:
            return p["name"]
    hits = [p["name"] for p in _PARSERS if q in p["name"].lower()]
    if len(hits) == 1:
        return hits[0]
    hits = [p["name"] for p in _PARSERS if p["name"].lower().startswith(q)]
    if len(hits) == 1:
        return hits[0]
    return query


# ═══════════════════════════════════════════════════════════════════
#  GAME PARSERS
# ═══════════════════════════════════════════════════════════════════

@game_parser("Wordle", r"Wordle\s+[\d,]+\s+([X\d])/6",
             lower_is_better=True, icon="🟩")
def _wordle(m):
    v = m.group(1).upper()
    if v == "X":
        return 7, 6, "X/6"
    return int(v), 6, f"{v}/6"


def parse_wordle_group_summary(text: str, msg_created_at=None) -> list[tuple]:
    """
    Parse Wordle group summary messages.
    Returns list of (username, score, max_score, display, game_date) tuples
    """
    # Check if this is a group summary message
    is_summary = "yesterday's results" in text.lower() or "today's results" in text.lower()
    
    if not is_summary:
        return []
    
    # Use the message date as the game date
    if msg_created_at:
        game_date = msg_created_at.astimezone(ZoneInfo("America/New_York")).date()
    else:
        game_date = datetime.now(ZoneInfo("America/New_York")).date()
    
    results = []
    # Pattern: optional 👑, then score/X: followed by names
    pattern = r'(?:👑\s*)?(\d|X)/(\d+):\s*(.+)'
    
    for line in text.split('\n'):
        match = re.search(pattern, line.strip())
        if match:
            score_str = match.group(1).upper()
            max_score = int(match.group(2))
            names_part = match.group(3)
            
            if score_str == "X":
                score = 7
            else:
                score = int(score_str)
            
            display = f"{score_str}/{max_score}"
            
            # Extract usernames - remove hashtags first, then get all @mentions and names
            names_clean = re.sub(r'#\S+', '', names_part).strip()
            users = []
            
            for part in names_clean.split():
                part = part.strip()
                if not part:
                    continue
                    
                # Remove @ prefix if present
                if part.startswith('@'):
                    username = part[1:]
                else:
                    username = part
                
                # Clean up trailing punctuation
                username = username.strip('.,!?;:')
                
                # Skip empty, hashtags, markdown, or single punctuation
                if username and len(username) > 1 and not username.startswith('#') and username != '**':
                    users.append(username)
            
            for username in users:
                if username and len(username) > 1:
                    results.append((username, score, max_score, display, game_date.isoformat()))
    
    return results


@game_parser("Pokédle", r"#Pokédle[\s\S]*?in\s+(one|\d+)\s+shots?",
             lower_is_better=True, icon="⚔️")
def _pokedle(m):
    raw = m.group(1)
    s = 1 if raw == "one" else int(raw)
    return s, 8, f"{s} shots"


@game_parser("LoLdle", r"#LoLdle[\s\S]*?in\s+(one|\d+)\s+shots?",
             lower_is_better=True, icon="🎮")
def _loldle(m):
    raw = m.group(1)
    s = 1 if raw == "one" else int(raw)
    return s, 8, f"{s} shots"


@game_parser("Narutodle", r"#Narutodle[\s\S]*?in\s+(one|\d+)\s+shots?",
             lower_is_better=True, icon="🍥")
def _narutodle(m):
    raw = m.group(1)
    s = 1 if raw == "one" else int(raw)
    return s, 8, f"{s} shots"


@game_parser("WhenTaken", r"#WhenTaken[\s\S]*?I scored\s+(\d+)/(\d+)",
             lower_is_better=False, icon="🗓️")
def _whentaken(m):
    s, mx = int(m.group(1)), int(m.group(2))
    return s, mx, f"{s}/{mx}"


@game_parser("Dialed", r"Dialed\s+Daily[\s\S]*?([\d.]+)/50",
             icon="📞")
def _dialed(m):
    s = float(m.group(1))
    return s, 50, f"{s}/50"


@game_parser("Catfishing", r"catfishing\.net\s*\n?\s*#?\d+\s*-\s*([\d.]+)/(\d+)",
             lower_is_better=False, icon="🐟")
def _catfishing(m):
    s, mx = float(m.group(1)), int(m.group(2))
    return s, mx, f"{s}/{mx}"


@game_parser("Feudle", r"Feudle[\s\S]*?Score:\s*(\d+)/(\d+)",
             icon="📖")
def _feudle(m):
    s, mx = int(m.group(1)), int(m.group(2))
    return s, mx, f"{s}/{mx}"


@game_parser("Doctordle", r"Doctordle\s*#\d+\s*\n([^\n]+)",
             icon="🏥")
def _doctordle(m):
    line = m.group(1)
    g = line.count("🟩")
    r = line.count("🟥")
    b = line.count("⬛")
    total = g + r + b
    if total == 0:
        return None
    return g, total, f"{g}/{total} solved"


@game_parser("TimeGuessr", r"TimeGuessr\s*#\d+[\s\S]*?([\d,]+)/([\d,]+)",
             icon="⏱️")
def _timeguessr(m):
    s  = int(m.group(1).replace(",", ""))
    mx = int(m.group(2).replace(",", ""))
    return s, mx, f"{s:,}/{mx:,}"


@game_parser("Framed", r"Framed\s*#(\d+)\s*\n([🎥🟥🟩⬛\s]+)",
             lower_is_better=True, icon="🎬")
def _framed(m):
    line = m.group(2).strip()
    green = line.count("🟩")
    if green == 0:
        score = 7
    else:
        squares = [c for c in line if c in ["🟥", "🟩", "⬛"]]
        try:
            score = squares.index("🟩") + 1
        except ValueError:
            score = 7
    return score, 6, f"{score}/6"


@game_parser("Birdle", r"USA\s+(?:Lower\s+48|World|UK)?\s*Birdle\s*\n(\d{4}-\d{2}-\d{2})\s*\n([🐦❌\n]+)",
             lower_is_better=True, icon="🐦")
def _birdle(m):
    # Parse the grid
    grid_text = m.group(2).strip()
    rows = [line.strip() for line in grid_text.split('\n') if line.strip()]
    
    # Check if they succeeded (any row with all 🐦, no ❌)
    succeeded = False
    success_row = 0
    
    for i, row in enumerate(rows, 1):
        if '❌' not in row and len(row) >= 4:
            # Row has no X's and at least 4 emojis - success!
            succeeded = True
            success_row = i
            break
    
    if succeeded:
        # Return the row number where they succeeded
        return success_row, 6, f"{success_row}/6"
    else:
        # Failed - return X/6 or 7/6 (like Wordle)
        return 7, 6, "X/6"


@game_parser("Costcodle", r"Costcodle\s*#\d+\s+(\d)/6",
             lower_is_better=True, icon="🛒")
def _costcodle(m):
    s = int(m.group(1))
    return s, 6, f"{s}/6"


# ═══════════════════════════════════════════════════════════════════
#  GAME LINKS
# ═══════════════════════════════════════════════════════════════════

GAME_LINKS = {
    "Wordle": "https://www.nytimes.com/games/wordle/index.html",
    "Pokédle": "https://pokedle.net/",
    "LoLdle": "https://loldle.net/",
    "Narutodle": "https://narutodle.net/",
    "WhenTaken": "https://whentaken.com/",
    "Dialed": "https://dialed.gg/",
    "Catfishing": "https://catfishing.net/",
    "Feudle": "https://feudlegame.com/",
    "Doctordle": "https://doctordle.org/",
    "TimeGuessr": "https://timeguessr.com/",
    "Framed": "https://framed.wtf/",
    "Costcodle": "https://costcodle.com/",
    "Birdle": "https://www.play-birdle.com/lower48/"
}

NO_CROWN_GAMES = {"doctordle", "loldle", "narutodle", "pokedle", "whentaken", "birdle"}

# Games that count for crowns but don't show in the breakdown
SIMPLE_CROWN_GAMES = {"wordle"}


# ═══════════════════════════════════════════════════════════════════
#  SCORING HELPERS
# ═══════════════════════════════════════════════════════════════════

def _normalize_game_name(name: str) -> str:
    """Normalize game name by removing accents and lowercasing"""
    normalized = unicodedata.normalize('NFKD', name)
    return ''.join(c for c in normalized if not unicodedata.combining(c)).lower()


def _compute_crowns(rows, guild_id: str = ""):
    by_gd: dict[tuple, list] = defaultdict(list)
    
    # Get crown reset date for this guild
    crown_reset_date = store.get_crown_reset_date(guild_id) if guild_id else None
    
    for r in rows:
        normalized_name = _normalize_game_name(r["game"])
        if normalized_name in NO_CROWN_GAMES:
            continue
        if r["score"] > r["max_score"]:
            continue
        
        # Skip results before crown reset date
        if crown_reset_date:
            result_date = datetime.strptime(r["puzzle_date"], "%Y-%m-%d").date()
            reset_date = crown_reset_date.date()
            if result_date < reset_date:
                continue
            
        by_gd[(r["game"], r["puzzle_date"])].append(r)

    user_crowns:   dict[tuple, int]       = defaultdict(int)
    daily_winners: dict[tuple, set[str]]  = {}

    for (game, date), gd_rows in by_gd.items():
        meta = _GAME_META.get(game.lower(), {})
        low  = meta.get("low", False)
        scores = [r["score"] for r in gd_rows]
        best   = min(scores) if low else max(scores)
        winners = set()
        for r in gd_rows:
            if r["score"] == best:
                user_crowns[(r["user_id"], game)] += 1
                winners.add(r["user_id"])
        daily_winners[(game, date)] = winners

    return user_crowns, daily_winners


def _rank_items(items, *, key, reverse=False):
    ordered = sorted(items, key=key, reverse=reverse)
    out, prev_val, prev_rank = [], None, 0
    for i, item in enumerate(ordered):
        val = key(item)
        if val != prev_val:
            prev_rank = i
            prev_val  = val
        medal = MEDALS[prev_rank] if prev_rank < len(MEDALS) else f"`{prev_rank+1}.`"
        out.append((prev_rank, medal, item))
    return out


def _fmt_avg(avg, max_score):
    if max_score >= 1000:
        return f"{avg:,.0f}/{max_score:,.0f}"
    elif max_score == int(max_score):
        return f"{avg:.1f}/{int(max_score)}"
    else:
        return f"{avg:.2f}/{max_score:.0f}"


# ═══════════════════════════════════════════════════════════════════
#  EMBED BUILDERS
# ═══════════════════════════════════════════════════════════════════

GAMES_PER_PAGE = 4


def _build_game_fields(rows) -> list[tuple[str, str]]:
    by_game: dict[str, list] = defaultdict(list)
    for r in rows:
        by_game[r["game"]].append(r)

    fields = []
    for game_name in sorted(by_game, key=lambda g: len(by_game[g]), reverse=True):
        gr   = by_game[game_name]
        meta = _GAME_META.get(game_name.lower(), {})
        low  = meta.get("low", False)

        ranked = _rank_items(gr, key=lambda r: r["score"], reverse=(not low))
        best_score     = ranked[0][2]["score"]     if ranked else None
        best_max_score = ranked[0][2]["max_score"] if ranked else None
        all_failed = best_score > best_max_score if best_score and best_max_score else False

        lines = []
        for _rank, medal, r in ranked:
            is_crown = r["score"] == best_score and len(gr) > 1 and not all_failed
            crown  = " 👑" if is_crown else ""
            indent = "╰ " if not is_crown and len(gr) > 1 else ""
            lines.append(f"{indent}{medal} **{r['username']}** — {r['display']}{crown}")

        fields.append((game_name, "\n".join(lines)))
    return fields


def _build_page_embed(title: str, fields: list[tuple[str, str]],
                      page: int, total_pages: int) -> discord.Embed:
    embed = discord.Embed(title=title, color=0x5865F2,
                          timestamp=datetime.now(timezone.utc))
    start = page * GAMES_PER_PAGE
    for name, value in fields[start:start + GAMES_PER_PAGE]:
        embed.add_field(name=name, value=value, inline=False)
    if total_pages > 1:
        embed.set_footer(text=f"Page {page + 1}/{total_pages}")
    return embed


class DailyLeaderboardView(discord.ui.View):
    def __init__(self, title: str, fields: list[tuple[str, str]]):
        super().__init__(timeout=300)
        self.title       = title
        self.fields      = fields
        self.page        = 0
        self.total_pages = max(1, (len(fields) + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.total_pages - 1

    def current_embed(self) -> discord.Embed:
        return _build_page_embed(self.title, self.fields, self.page, self.total_pages)

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)


def _build_daily_embed(title, rows):
    fields = _build_game_fields(rows)
    if not fields:
        embed = discord.Embed(title=title, color=0x5865F2,
                              timestamp=datetime.now(timezone.utc))
        embed.description = "No results yet — paste a game share to get started!"
        return embed, None
    total_pages = max(1, (len(fields) + GAMES_PER_PAGE - 1) // GAMES_PER_PAGE)
    embed = _build_page_embed(title, fields, 0, total_pages)
    view  = DailyLeaderboardView(title, fields) if total_pages > 1 else None
    return embed, view


def _build_period_embed(title, rows, *, show_detail=False, guild_id: str = ""):
    by_game: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        by_game[r["game"]][r["user_id"]].append(r)

    embed = discord.Embed(title=title, color=0x5865F2,
                          timestamp=datetime.now(timezone.utc))
    if not by_game:
        embed.description = "No results for this period!"
        return embed

    user_crowns, daily_winners = _compute_crowns(rows, guild_id)

    for game_name in sorted(by_game):
        users = by_game[game_name]
        meta  = _GAME_META.get(game_name.lower(), {})
        low   = meta.get("low", False)
        icon  = meta.get("icon", "🎮")

        stats = []
        for uid, urows in users.items():
            avg  = sum(r["score"] for r in urows) / len(urows)
            mx   = urows[0]["max_score"]
            best = min(r["score"] for r in urows) if low \
                   else max(r["score"] for r in urows)
            best_disp = next(r["display"] for r in urows if r["score"] == best)
            stats.append(dict(
                uid=uid, name=urows[0]["username"], avg=avg,
                max_score=mx, count=len(urows),
                crowns=user_crowns.get((uid, game_name), 0),
                best=best, best_disp=best_disp, rows=urows))

        ranked = _rank_items(stats, key=lambda p: p["avg"],
                             reverse=(not low))

        lines = []
        for _rank, medal, p in ranked:
            avg_s   = _fmt_avg(p["avg"], p["max_score"])
            crown_s = f" · 👑{p['crowns']}" if p["crowns"] else ""
            lines.append(
                f"{medal} **{p['name']}** — avg {avg_s} "
                f"({p['count']}d{crown_s})")

            if show_detail:
                day_parts = []
                for r in sorted(p["rows"],
                                key=lambda r: r["puzzle_date"]):
                    d = datetime.strptime(r["puzzle_date"], "%Y-%m-%d")
                    lbl = d.strftime("%b %d")
                    won = r["user_id"] in daily_winners.get(
                        (game_name, r["puzzle_date"]), set())
                    day_parts.append(
                        f"{lbl}: {r['display']}{' 👑' if won else ''}")
                lines.append(f"╰ {' · '.join(day_parts)}")

        field_name = f"{icon}  {game_name}"
        if low:
            field_name += " (lower is better)"
        else:
            field_name += " (higher is better)"
            
        embed.add_field(name=field_name,
                        value="\n".join(lines) or "—", inline=False)
    return embed


def _add_streaks(embed, rows):
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=90)).isoformat()
    recent_rows = [r for r in rows if r["puzzle_date"] >= cutoff]
    
    ud: dict[str, set[str]] = defaultdict(set)
    un: dict[str, str]      = {}
    for r in recent_rows:
        ud[r["user_id"]].add(r["puzzle_date"])
        un[r["user_id"]] = r["username"]

    today   = datetime.now(timezone.utc).date()
    streaks = []
    for uid, raw in ud.items():
        ds = sorted((datetime.strptime(d, "%Y-%m-%d").date()
                      for d in raw), reverse=True)
        if not ds or ds[0] < today - timedelta(days=1):
            continue
        s = 1
        for j in range(1, len(ds)):
            if ds[j] == ds[j-1] - timedelta(days=1):
                s += 1
            else:
                break
        if s >= 2:
            streaks.append((un[uid], s))

    if streaks:
        streaks.sort(key=lambda x: x[1], reverse=True)
        val = "\n".join(
            f"**{n}** — {s} day{'s'*(s!=1)} 🔥" for n, s in streaks)
        embed.add_field(name="🔥  Streaks", value=val, inline=False)


# ═══════════════════════════════════════════════════════════════════
#  HISTORY SYNC
# ═══════════════════════════════════════════════════════════════════

async def sync_history_to_store(channel: discord.TextChannel, days: int = 30):
    """Fetch message history and populate the store"""
    gid = str(channel.guild.id)
    
    if days == 1:
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today_start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        after = today_start_et.astimezone(timezone.utc)
    else:
        after = datetime.now(timezone.utc) - timedelta(days=days)
    
    # Check crown reset date - only fetch from after reset
    crown_reset_date = store.get_crown_reset_date(gid)
    if crown_reset_date:
        reset_utc = crown_reset_date.astimezone(timezone.utc)
        if reset_utc > after:
            after = reset_utc
            log.info(f"Using crown reset date {after} as fetch cutoff")
    
    message_count = 0
    result_count = 0
    
    try:
        async for msg in channel.history(limit=2000, after=after, oldest_first=False):
            # Skip bot messages except Wordle app
            author_name = msg.author.name.lower()
            is_wordle_bot = msg.author.bot and ("wordle" in author_name or author_name.startswith("wordle"))
            if msg.author.bot and not is_wordle_bot:
                continue
            
            # Skip messages before crown reset date
            if crown_reset_date:
                msg_date = msg.created_at.astimezone(ZoneInfo("America/New_York")).date()
                reset_date = crown_reset_date.date()
                if msg_date < reset_date:
                    continue
            
            # Check for Wordle group summary
            wordle_results = parse_wordle_group_summary(msg.content, msg.created_at)
            if wordle_results:
                for username, score, max_score, display, game_date in wordle_results:
                    result = GameResult("Wordle", score, max_score, display)
                    # Look up real user_id from name mapping
                    user_id = store.get_user_id_from_name(gid, username)
                    store.save(gid, user_id, username, result, game_date)
                    result_count += 1
            else:
                # Normal parsing
                results = parse_message(msg.content)
                if results:
                    # Convert to Eastern Time for consistent date handling
                    date_str = msg.created_at.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
                    uid = str(msg.author.id)
                    name = msg.author.display_name
                    
                    for r in results:
                        store.save(gid, uid, name, r, date_str)
                        # Map name to user_id for Wordle lookups
                        store.map_name_to_id(gid, name, uid)
                        result_count += 1
            message_count += 1
    except discord.HTTPException as e:
        log.error("Error fetching history: %s", e)
    
    log.info(f"Synced {message_count} messages, found {result_count} game results")
    return result_count


# ═══════════════════════════════════════════════════════════════════
#  DISCORD BOT
# ═══════════════════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

config_store: dict[str, Optional[int]] = {}


@bot.event
async def on_ready():
    if store.check_reset():
        log.info("🗑️  Monthly storage reset performed")
    
    if not daily_summary.is_running():
        daily_summary.start()
    if not monthly_reset_check.is_running():
        monthly_reset_check.start()
    if not daily_links.is_running():
        daily_links.start()
    
    try:
        synced = await bot.tree.sync()
        log.info("🔄  Synced %d slash commands", len(synced))
    except Exception as e:
        log.warning("Slash command sync failed: %s", e)

    cmds = [c.name for c in bot.commands]
    log.info("✅  %s online  ·  tracking %d games  ·  prefix: '%s'  ·  commands: %s",
             bot.user, len(_PARSERS), PREFIX, cmds)


@bot.event
async def on_command_error(ctx, error):
    log.info("CMD ERROR in %s: %s", ctx.command, error)


@bot.event
async def on_message(msg: discord.Message):
    if not msg.guild:
        return
    
    # Allow Wordle bot messages for group summaries
    author_name = msg.author.name.lower()
    is_wordle_bot = msg.author.bot and ("wordle" in author_name or author_name.startswith("wordle"))
    if msg.author.bot and not is_wordle_bot:
        return
    
    store.check_reset()
    
    # Check if message is before crown reset date - don't save old data
    gid = str(msg.guild.id)
    crown_reset_date = store.get_crown_reset_date(gid)
    if crown_reset_date:
        msg_date = msg.created_at.astimezone(ZoneInfo("America/New_York")).date()
        reset_date = crown_reset_date.date()
        if msg_date < reset_date:
            log.info("Skipping old message from %s (before crown reset %s)", 
                     msg_date, reset_date)
            return
    
    log.info("MSG from %s: %s", msg.author.display_name, msg.content[:80])
     
    # Check for Wordle group summary
    wordle_results = parse_wordle_group_summary(msg.content, msg.created_at)
    if wordle_results:
        gid = str(msg.guild.id)
        for username, score, max_score, display, game_date in wordle_results:
            # Create a GameResult
            result = GameResult("Wordle", score, max_score, display)
            # Look up real user_id from name mapping, or use name if not found
            user_id = store.get_user_id_from_name(gid, username)
            store.save(gid, user_id, username, result, game_date)
        await msg.add_reaction("📊")
    else:
        # Normal parsing for user messages
        results = parse_message(msg.content)
        if results:
            # Convert to Eastern Time for consistent date handling
            d = msg.created_at.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
            gid  = str(msg.guild.id)
            uid  = str(msg.author.id)
            name = msg.author.display_name
            for r in results:
                store.save(gid, uid, name, r, d)
                # Also map this name to the user_id for future Wordle lookups
                store.map_name_to_id(gid, name, uid)
            await msg.add_reaction("📊")
    
    try:
        await bot.process_commands(msg)
    except Exception as e:
        log.info("COMMAND ERROR: %s", e)


# ═══════════════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════════════

PERIODS = {"today", "yesterday", "yday", "week", "month", "all"}

PeriodChoice = Literal["today", "yesterday", "week", "month", "all"]


async def game_autocomplete(interaction: discord.Interaction, current: str):
    return [
        app_commands.Choice(name=p["name"], value=p["name"])
        for p in _PARSERS
        if current.lower() in p["name"].lower()
    ][:25]


@bot.hybrid_command(name="synclb")
@app_commands.describe(days="Number of days of history to sync (default 30)")
async def cmd_sync(ctx, days: int = 30):
    """Manually sync message history"""
    await ctx.send(f"🔄 Syncing last {days} days...")
    count = await sync_history_to_store(ctx.channel, days)
    await ctx.send(f"✅ Found {count} game results!")


@bot.hybrid_command(name="links")
async def cmd_links(ctx):
    """Show all game links with crown info"""
    crown_games = []
    no_crown_games = []
    
    for game_name, url in sorted(GAME_LINKS.items()):
        meta = _GAME_META.get(game_name.lower(), {})
        icon = meta.get('icon', '🎮')
        
        # Check if this game awards crowns (use normalized name)
        normalized = _normalize_game_name(game_name)
        if normalized in NO_CROWN_GAMES:
            no_crown_games.append(f"{icon} [{game_name}]({url})")
        elif normalized in SIMPLE_CROWN_GAMES:
            # Wordle - show in crowns section but without link (run by app)
            crown_games.append(f"👑 {icon} {game_name}")
        else:
            crown_games.append(f"👑 {icon} [{game_name}]({url})")
    
    lines = ["**👑 Games Award Crowns:**"] + crown_games + ["", "**Games for Fun (No Crowns):**"] + no_crown_games
    
    embed = discord.Embed(
        title="🎮  Daily Games Links",
        description="\n".join(lines),
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc)
    )
    await ctx.send(embed=embed)


@bot.hybrid_command(name="lb", aliases=["leaderboard"])
@app_commands.describe(period="Time period to show", game="Filter by a specific game")
@app_commands.autocomplete(game=game_autocomplete)
async def cmd_lb(ctx, period: PeriodChoice = "today", game: Optional[str] = None):
    gid   = str(ctx.guild.id)
    today = datetime.now(ZoneInfo("America/New_York")).date()

    game_name = _match_game(game) if game else None

    kw: dict     = {}
    single_day   = False

    if period == "today":
        kw["date"] = today.isoformat()
        title = f"📊  Today — {today.strftime('%b %d')}"
        single_day = True
    elif period in ("yesterday", "yday"):
        yd = today - timedelta(days=1)
        kw["date"] = yd.isoformat()
        title = f"📊  Yesterday — {yd.strftime('%b %d')}"
        single_day = True
    elif period == "week":
        mon = today - timedelta(days=today.weekday())
        kw["start"], kw["end"] = mon.isoformat(), today.isoformat()
        title = (f"📊  This Week  "
                 f"({mon.strftime('%b %d')} – {today.strftime('%b %d')})")
    elif period == "month":
        kw["start"] = today.replace(day=1).isoformat()
        kw["end"]   = today.isoformat()
        title = f"📊  {today.strftime('%B %Y')}"
    else:
        title = "📊  All Time"

    if game_name:
        kw["game"] = game_name
        meta = _GAME_META.get(game_name.lower(), {})
        title += f"  ·  {meta.get('icon','🎮')} {game_name}"

    if single_day and isinstance(ctx.channel, discord.TextChannel):
        await sync_history_to_store(ctx.channel, days=1)
    
    rows = store.fetch(gid, **kw)

    if single_day:
        embed, view = _build_daily_embed(title, rows)
        if single_day and not game_name:
            all_rows = store.get_all(gid)
            _add_streaks(embed, all_rows)
        await ctx.send(embed=embed, view=view)
    else:
        show_detail = bool(game_name) and period == "week"
        embed = _build_period_embed(title, rows, show_detail=show_detail)
        await ctx.send(embed=embed)


@bot.hybrid_command(name="games")
async def cmd_games(ctx):
    lines = []
    for p in sorted(_PARSERS, key=lambda p: p["name"]):
        d = "fewer = better" if p["low"] else "higher = better"
        lines.append(f"{p['icon']}  **{p['name']}** — {d}")
    e = discord.Embed(title="🎲  Supported Games", color=0x57F287,
                      description="\n".join(lines))
    e.set_footer(text="Paste a game's share text and I'll track it automatically!")
    await ctx.send(embed=e)


@bot.hybrid_command(name="mystats", aliases=["stats"])
@app_commands.describe(member="User to show stats for (default: yourself)")
async def cmd_stats(ctx, member: Optional[discord.Member] = None):
    target = member or ctx.author
    gid    = str(ctx.guild.id)
    uid    = str(target.id)
    
    rows = store.fetch(gid, uid=uid)
    if not rows:
        return await ctx.send(f"No results for **{target.display_name}** yet.")

    all_rows      = store.get_all(gid)
    crowns_map, _ = _compute_crowns(all_rows, gid)

    by_game: dict[str, list] = defaultdict(list)
    for r in rows:
        by_game[r["game"]].append(r)

    total_plays  = 0
    total_crowns = 0

    e = discord.Embed(title=f"📈  {target.display_name}",
                      color=0xFEE75C,
                      timestamp=datetime.now(timezone.utc))

    for gn in sorted(by_game):
        gr   = by_game[gn]
        meta = _GAME_META.get(gn.lower(), {})
        low  = meta.get("low", False)
        icon = meta.get("icon", "🎮")

        avg  = sum(r["score"] for r in gr) / len(gr)
        mx   = gr[0]["max_score"]
        best = min(r["score"] for r in gr) if low \
               else max(r["score"] for r in gr)
        best_d = next(r["display"] for r in gr if r["score"] == best)
        c = crowns_map.get((uid, gn), 0)

        total_plays  += len(gr)
        total_crowns += c

        avg_s   = _fmt_avg(avg, mx)
        crown_s = f" · 👑 {c}" if c else ""
        direction = "lower=better" if low else "higher=better"
        
        e.add_field(
            name=f"{icon}  {gn}  ({direction})",
            value=(f"Avg **{avg_s}** · Best **{best_d}** · "
                   f"{len(gr)} plays{crown_s}"),
            inline=False)

    dates = set(r["puzzle_date"] for r in rows)
    e.description = (f"**{total_plays}** plays across **{len(dates)}** days "
                   f"· **{total_crowns}** 👑 total")
    await ctx.send(embed=e)


@bot.hybrid_command(name="crowns")
@app_commands.describe(period="Time period to show", game="Filter by a specific game")
@app_commands.autocomplete(game=game_autocomplete)
async def cmd_crowns(ctx, period: PeriodChoice = "month", game: Optional[str] = None):
    gid   = str(ctx.guild.id)
    today = datetime.now(ZoneInfo("America/New_York")).date()

    game_name = _match_game(game) if game else None

    kw: dict = {}
    if period == "today":
        kw["date"] = today.isoformat()
    elif period in ("yesterday", "yday"):
        kw["date"] = (today - timedelta(days=1)).isoformat()
    elif period == "week":
        mon = today - timedelta(days=today.weekday())
        kw["start"], kw["end"] = mon.isoformat(), today.isoformat()
    elif period == "month":
        kw["start"] = today.replace(day=1).isoformat()
        kw["end"]   = today.isoformat()
    if game_name:
        kw["game"] = game_name

    if isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("🔄 Fetching leaderboard history...", delete_after=3)
        days_to_sync = 1 if period == "today" else (7 if period == "week" else 30)
        await sync_history_to_store(ctx.channel, days=days_to_sync)
    
    rows = store.fetch(gid, **kw)
    crowns_map, _ = _compute_crowns(rows, gid)

    names: dict[str, str] = {}
    for r in rows:
        names[r["user_id"]] = r["username"]

    user_agg: dict[str, dict] = defaultdict(
        lambda: {"total": 0, "by_game": defaultdict(int), "name": ""})
    for (uid, gn), c in crowns_map.items():
        user_agg[uid]["total"]        += c
        user_agg[uid]["by_game"][gn]  += c
        user_agg[uid]["name"]          = names.get(uid, "?")

    ranked = _rank_items(list(user_agg.items()),
                         key=lambda x: x[1]["total"], reverse=True)

    title = "👑  Crown Leaderboard"
    if game_name:
        meta = _GAME_META.get(game_name.lower(), {})
        title += f" · {meta.get('icon','🎮')} {game_name}"
    if period != "month":
        title += f" · {period.title()}"

    e = discord.Embed(title=title, color=0xFFD700,
                      timestamp=datetime.now(timezone.utc))
    if not ranked:
        e.description = "No crowns earned yet!"
        return await ctx.send(embed=e)

    lines = []
    for _rank, medal, (uid, data) in ranked[:15]:
        game_parts = []
        displayed_crowns = 0
        wordle_crowns = 0
        
        for g, c in sorted(data["by_game"].items(), key=lambda x: x[1], reverse=True):
            if g.lower() in SIMPLE_CROWN_GAMES:
                wordle_crowns += c
                continue
            meta = _GAME_META.get(g.lower(), {})
            icon = meta.get('icon', '🎮')
            game_parts.append(f"{icon} {g}: {c}")
            displayed_crowns += c
        
        # Add Wordle if there are hidden crowns
        if wordle_crowns > 0:
            game_parts.append(f"🟩 Wordle: {wordle_crowns}")
        
        breakdown = " · ".join(game_parts) if game_parts else "No detailed breakdown"
        lines.append(
            f"{medal} **{data['name']}** — **{data['total']}** crowns\n"
            f"╰ {breakdown}")

    e.description = "\n".join(lines)
    await ctx.send(embed=e)


@bot.hybrid_command(name="resetcrowns")
@commands.has_permissions(manage_guild=True)
@app_commands.describe(confirm="Type 'confirm' to proceed", date_str="Optional start date (e.g. 3-26-2025)")
async def cmd_reset_crowns(ctx, confirm: str = "", date_str: str = ""):
    """Reset crown counts to 0 - crowns will only count from specified date (or today) forward"""
    if confirm.lower() != "confirm":
        await ctx.send(
            "⚠️ **Warning**: This will reset ALL crown counts to 0.\n"
            "Leaderboard data will be preserved, but crowns will only count from now forward.\n"
            "To confirm, type: `!zg resetcrowns confirm`\n"
            "To reset to a specific date: `!zg resetcrowns confirm 3-26-2025`\n\n"
            "*Requires 'Manage Server' permission.*"
        )
        return
    
    gid = str(ctx.guild.id)
    
    # Parse date if provided, otherwise use today
    reset_date = None
    if date_str:
        try:
            # Try various date formats: M-D-YYYY, M/D/YYYY, M-D-YY, etc.
            for fmt in ["%m-%d-%Y", "%m/%d/%Y", "%m-%d-%y", "%m/%d/%y"]:
                try:
                    parsed = datetime.strptime(date_str, fmt)
                    # Assume current century for 2-digit years
                    if parsed.year < 100:
                        parsed = parsed.replace(year=parsed.year + 2000)
                    reset_date = parsed.date()
                    break
                except ValueError:
                    continue
            
            if not reset_date:
                await ctx.send(f"❌ Invalid date format: `{date_str}`. Use format like `3-26-2025` or `3/26/2025`")
                return
            
            # Set to midnight ET on that date
            reset_datetime = datetime.combine(reset_date, dt_time(0, 0, 0))
            reset_datetime = reset_datetime.replace(tzinfo=ZoneInfo("America/New_York"))
        except Exception as e:
            await ctx.send(f"❌ Error parsing date: {e}")
            return
    else:
        # Use today (default behavior)
        reset_datetime = None
    
    # Clear ALL existing data for this guild from the store
    old_keys = [k for k, r in store.results.items() if r["guild_id"] == gid]
    for k in old_keys:
        del store.results[k]
    
    # Set the crown reset date
    if reset_datetime:
        store.crown_reset_dates[gid] = reset_datetime
        formatted_date = reset_datetime.strftime("%B %d, %Y")
        log.info(f"👑 Crown reset by {ctx.author.name} for guild {gid} to date {reset_datetime}, cleared {len(old_keys)} old entries")
    else:
        reset_datetime = store.reset_crowns(gid)
        formatted_date = reset_datetime.strftime("%B %d, %Y")
        log.info(f"👑 Crown reset by {ctx.author.name} for guild {gid} at {reset_datetime}, cleared {len(old_keys)} old entries")
    
    await ctx.send(f"👑 **Crowns Reset**: Crown counts reset to 0!\n"
                   f"Cleared {len(old_keys)} old results.\n"
                   f"Crowns will now only count from **{formatted_date}** forward.")


@bot.hybrid_command(name="reconcile")
@commands.has_permissions(manage_guild=True)
async def cmd_reconcile(ctx):
    """Manually reconcile all Wordle entries to merge duplicates"""
    gid = str(ctx.guild.id)
    
    # Count current state
    before_count = len([r for r in store.results.values() if r["guild_id"] == gid])
    
    # Reconcile all names in the mapping
    reconciled = 0
    for (g, name), user_id in list(store.name_to_id.items()):
        if g == gid:
            store._reconcile_wordle_entries(gid, name, user_id)
            reconciled += 1
    
    # Also try to discover mappings from existing non-Wordle entries
    discovered = 0
    for r in store.results.values():
        if r["guild_id"] == gid and r["game"] != "Wordle":
            # This has a real user_id, check if we can map the username
            username = r["username"]
            user_id = r["user_id"]
            if username != user_id and (gid, username) not in store.name_to_id:
                # Found a mapping we didn't have
                store.map_name_to_id(gid, username, user_id)
                discovered += 1
    
    after_count = len([r for r in store.results.values() if r["guild_id"] == gid])
    
    await ctx.send(f"🔄 **Reconciliation Complete**\n"
                   f"• Reconciled {reconciled} known name mappings\n"
                   f"• Discovered {discovered} new name mappings\n"
                   f"• Results: {before_count} → {after_count} entries\n\n"
                   f"Duplicates should now be merged in crown leaderboard!")


@bot.hybrid_command(name="debug")
@commands.has_permissions(manage_guild=True)
async def cmd_debug(ctx):
    """Debug command to see all users in the store"""
    gid = str(ctx.guild.id)
    
    # Get all unique user_ids for this guild
    users = {}
    for r in store.results.values():
        if r["guild_id"] == gid:
            uid = r["user_id"]
            name = r["username"]
            if uid not in users:
                users[uid] = {"name": name, "games": set()}
            users[uid]["games"].add(r["game"])
    
    if not users:
        return await ctx.send("No data found in store for this guild.")
    
    lines = ["**Users in store:**"]
    for uid, data in sorted(users.items()):
        games_list = ", ".join(sorted(data["games"]))
        lines.append(f"• `{uid}` ({data['name']}) - {games_list}")
    
    await ctx.send("\n".join(lines[:20]))  # Limit to 20 users


@bot.hybrid_command(name="merge")
@commands.has_permissions(manage_guild=True)
@app_commands.describe(source="User ID or mention to merge from", target="User ID or mention to merge into")
async def cmd_merge(ctx, source: str, target: str):
    """Manually merge two user entries (source -> target). Use Discord IDs or usernames."""
    gid = str(ctx.guild.id)
    
    # Helper to extract ID from mention format <@123456789>
    def extract_id(s):
        if s.startswith('<@') and s.endswith('>'):
            return s[2:-1]  # Remove <@ and >
        if s.startswith('<@!'):
            return s[3:-1]  # Remove <@! and > (nickname mention)
        return s
    
    source_clean = extract_id(source)
    target_clean = extract_id(target)
    
    # Also try with/without brackets
    source_variants = [source_clean, f"<@{source_clean}>", f"<@!{source_clean}>"]
    target_variants = [target_clean, f"<@{target_clean}>", f"<@!{target_clean}>"]
    
    # Try to find source by any variant
    source_keys = []
    found_source_format = None
    
    for variant in source_variants:
        keys = [k for k, r in store.results.items() 
                if r["guild_id"] == gid and r["user_id"] == variant]
        if keys:
            source_keys = keys
            found_source_format = variant
            break
    
    if not source_keys:
        # Show debug info
        users = {}
        for r in store.results.values():
            if r["guild_id"] == gid:
                uid = r["user_id"]
                name = r["username"]
                if uid not in users:
                    users[uid] = name
        
        lines = ["❌ No data found for user. **Available users:**"]
        for uid, name in sorted(users.items())[:15]:
            lines.append(f"• `{uid}` ({name})")
        
        return await ctx.send("\n".join(lines))
    
    # Find target
    target_final = target_clean
    target_name = target_clean
    
    for variant in target_variants:
        for r in store.results.values():
            if r["guild_id"] == gid and r["user_id"] == variant:
                target_final = variant
                target_name = r["username"]
                break
        if target_final != target_clean:
            break
    
    # Merge
    merged = 0
    for key in source_keys:
        r = store.results[key]
        new_key = (gid, target_final, r["game"], r["puzzle_date"])
        
        if new_key in store.results:
            existing = store.results[new_key]
            meta = _GAME_META.get(r["game"].lower(), {})
            low = meta.get("low", False)
            
            if low:
                if r["score"] < existing["score"]:
                    store.results[new_key] = {**r, "user_id": target_final, "username": target_name}
            else:
                if r["score"] > existing["score"]:
                    store.results[new_key] = {**r, "user_id": target_final, "username": target_name}
        else:
            store.results[new_key] = {**r, "user_id": target_final, "username": target_name}
        
        del store.results[key]
        merged += 1
    
    # Create all mappings
    for variant in source_variants:
        store.name_to_id[(gid, variant)] = target_final
    
    await ctx.send(f"✅ **Merged {merged} entries**\n"
                   f"• Source format: `{found_source_format}`\n"
                   f"• Target: `{target_final}` ({target_name})\n\n"
                   f"Run `!zg crowns` to see updated leaderboard!")


@bot.hybrid_command(name="setchannel")
@commands.has_permissions(manage_guild=True)
@app_commands.describe(channel="Channel for daily recaps (leave blank to disable)")
async def cmd_setchannel(ctx, channel: Optional[discord.TextChannel] = None):
    gid = str(ctx.guild.id)
    config_store[gid] = channel.id if channel else None
    
    if channel:
        await ctx.send(f"✅  Daily recaps → {channel.mention}")
    else:
        await ctx.send("✅  Daily recaps disabled.")


@bot.hybrid_command(name="help")
async def cmd_help(ctx):
    e = discord.Embed(
        title="🎲  Zaily Games Leaderboard",
        color=0x5865F2,
        description=(
            "I track daily puzzle games automatically!\n"
            "Just paste your game's share text — I react 📊 when tracked."))
    e.add_field(name="📊  Leaderboards", inline=False, value=(
        f"`{PREFIX}lb` — today\n"
        f"`{PREFIX}lb yesterday`\n"
        f"`{PREFIX}lb week` / `month` / `all`\n"
        f"`{PREFIX}lb wordle` — one game, today"))
    e.add_field(name="👑  Crowns & Stats", inline=False, value=(
        f"`{PREFIX}crowns` — crown leaderboard (default: month)\n"
        f"`{PREFIX}crowns week` / `{PREFIX}crowns wordle`\n"
        f"`{PREFIX}mystats` / `{PREFIX}stats @user`\n"
        f"`{PREFIX}resetcrowns confirm` — 👑 reset crown counts (admin only)\n"
        f"`{PREFIX}resetcrowns confirm 3-26-2025` — reset to specific date"))
    e.add_field(name="⚙️  Admin Commands", inline=False, value=(
        f"`{PREFIX}setchannel #channel` — auto daily leaderboard at 11 PM ET\n"
        f"`{PREFIX}setchannel` — disable auto-post\n"
        f"`{PREFIX}resetcrowns confirm` — reset crown competition\n"
        f"`{PREFIX}reconcile` — auto-merge duplicate entries\n"
        f"`{PREFIX}merge @user1 user2_id` — manually merge duplicates\n\n"
        f"🗑️  **Auto-reset**: Leaderboards reset monthly on day {RESET_DAY}"))
    e.add_field(name="📚  Other Commands", inline=False, value=(
        f"`{PREFIX}games` — list supported games\n"
        f"`{PREFIX}links` — show all game links\n"
        f"`{PREFIX}sync` — manually sync message history"))
    e.set_footer(text="Add new games → edit GAME PARSERS in bot.py")
    await ctx.send(embed=e)


# ═══════════════════════════════════════════════════════════════════
#  DAILY RECAP & MONTHLY RESET
# ═══════════════════════════════════════════════════════════════════

@tasks.loop(time=dt_time(hour=23, minute=0, tzinfo=ZoneInfo("America/New_York")))
async def daily_summary():
    """Post daily leaderboard recap to configured channels"""
    today = datetime.now(ZoneInfo("America/New_York")).date()
    ds = today.isoformat()
    
    for guild in bot.guilds:
        gid = str(guild.id)
        channel_id = config_store.get(gid)
        
        if not channel_id:
            continue
            
        ch = bot.get_channel(channel_id)
        if not ch:
            continue
        
        rows = store.fetch(gid, date=ds)
        if not rows:
            continue
            
        embed, view = _build_daily_embed(
            f"📊  Daily Leaderboard — {today.strftime('%b %d')}", rows)

        all_rows = store.get_all(gid)
        _add_streaks(embed, all_rows)

        await ch.send(embed=embed, view=view)
        log.info("Recap sent → %s", guild.name)


@tasks.loop(time=dt_time(hour=0, minute=5, tzinfo=ZoneInfo("America/New_York")))
async def monthly_reset_check():
    """Check and perform monthly reset"""
    if store.check_reset():
        log.info("🗑️  Scheduled monthly reset completed")
        for guild in bot.guilds:
            gid = str(guild.id)
            channel_id = config_store.get(gid)
            if channel_id:
                ch = bot.get_channel(channel_id)
                if ch:
                    try:
                        await ch.send("🗑️ **Monthly Reset**: Leaderboard data has been cleared for the new month!")
                    except:
                        pass


@tasks.loop(time=dt_time(hour=8, minute=0, tzinfo=ZoneInfo("America/New_York")))
async def daily_links():
    """Post daily game links at 8AM ET"""
    today_str = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    
    for guild in bot.guilds:
        gid = str(guild.id)
        
        channel_id = config_store.get(gid)
        
        if not channel_id:
            continue
            
        ch = bot.get_channel(channel_id)
        if not ch:
            continue
        
        lines = []
        for game_name, url in sorted(GAME_LINKS.items()):
            meta = _GAME_META.get(game_name.lower(), {})
            icon = meta.get('icon', '🎮')
            lines.append(f"{icon} [{game_name}]({url})")
        
        embed = discord.Embed(
            title="🎮  Daily Games Links",
            description="\n".join(lines),
            color=0x57F287,
            timestamp=datetime.now(timezone.utc)
        )
        today = datetime.now(ZoneInfo("America/New_York"))
        embed.set_footer(text=f"{today.strftime('%A, %B %d')} · Good luck!")
        
        sent_msg = await ch.send(embed=embed)
        log.info("Links sent → %s", guild.name)
        
        # Check for and delete duplicate links posted today (from other instances)
        try:
            async for msg in ch.history(limit=20):
                if (msg.id != sent_msg.id and  # Don't delete the one we just posted
                    msg.author.id == bot.user.id and 
                    msg.embeds and 
                    msg.embeds[0].title == "🎮  Daily Games Links"):
                    msg_date = msg.created_at.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
                    if msg_date == today_str:
                        log.info(f"Deleting duplicate links message from today in {guild.name}")
                        await msg.delete()
                        break  # Only delete one duplicate
        except Exception as e:
            log.error(f"Error cleaning up duplicates: {e}")


# ═══════════════════════════════════════════════════════════════════
#  START
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DAILY_GAMES_BOT_TOKEN (env var or .env file)")
    bot.run(TOKEN)
