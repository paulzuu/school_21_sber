#!/usr/bin/env python

import os
import sys
import requests
import pytest
import collections
import re
from datetime import datetime
from bs4 import BeautifulSoup


# -----------------------------
# Helpers
# -----------------------------

def _safe_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def _parse_csv_line_relaxed(line: str):
    """
    Minimal CSV parsing for MovieLens:
    - supports quoted fields with commas inside
    - returns list[str]
    Never raises.
    """
    try:
        parts = re.findall(r'"([^"]*)"|([^,]+)', line.strip())
        return [p[0] if p[0] else p[1] for p in parts]
    except Exception:
        return []


def _read_first_1k(file_path: str, has_header: bool = True):
    """Read up to first 1000 data lines, safe."""
    try:
        lines = []
        with open(file_path, "r", encoding="utf-8") as f:
            if has_header:
                f.readline()
            for _ in range(1000):
                line = f.readline()
                if not line:
                    break
                lines.append(line.rstrip("\n"))
        return lines
    except Exception:
        return []


def _extract_number(text: str):
    """Extract a float number from strings like '$1,234,567'."""
    try:
        if not text or text == "N/A":
            return 0.0
        m = re.search(r'([\d,]+(?:\.\d+)?)', text)
        if not m:
            return 0.0
        return float(m.group(1).replace(",", ""))
    except Exception:
        return 0.0


def _runtime_to_minutes(text: str):
    """Parse runtime strings like '120 min' or '2h 5m' into minutes."""
    try:
        if not text or text == "N/A":
            return 0
        m = re.search(r'(\d+)\s*min', text)
        if m:
            return _safe_int(m.group(1), 0) or 0
        m = re.search(r'(\d+)\s*h\s*(\d+)\s*m', text)
        if m:
            h = _safe_int(m.group(1), 0) or 0
            mm = _safe_int(m.group(2), 0) or 0
            return h * 60 + mm
        return 0
    except Exception:
        return 0


# -----------------------------
# Core classes
# -----------------------------

class Movies:
    """
    Analyzing data from movies.csv
    """
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.movies_1k = []

        for line in _read_first_1k(file_path, has_header=True):
            parts = _parse_csv_line_relaxed(line)
            if len(parts) >= 3:
                self.movies_1k.append({
                    "movieId": parts[0],
                    "title": parts[1].strip(),
                    "genres": parts[2]
                })

    def dist_by_release(self) -> dict:
        """{year(str): count(int)} sorted by count desc."""
        try:
            years = []
            for m in self.movies_1k:
                title = m.get("title", "")
                year = re.search(r"\((\d{4})\)", title)
                if year:
                    years.append(year.group(1))
            c = collections.Counter(years)
            return dict(sorted(c.items(), key=lambda x: x[1], reverse=True))
        except Exception:
            return {}

    def dist_by_genres(self) -> dict:
        """{genre(str): count(int)} sorted by count desc."""
        try:
            genres = []
            for m in self.movies_1k:
                g = m.get("genres", "")
                if g:
                    genres.extend(g.split("|"))
            c = collections.Counter(genres)
            return dict(sorted(c.items(), key=lambda x: x[1], reverse=True))
        except Exception:
            return {}

    def most_genres(self, n: int) -> dict:
        """{title(str): num_genres(int)} sorted by num_genres desc."""
        try:
            counts = {}
            for m in self.movies_1k:
                title = m.get("title", "")
                g = m.get("genres", "")
                counts[title] = len(g.split("|")) if g else 0
            top = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)
        except Exception:
            return {}

    def get_movie_titles(self) -> dict:
        """{movieId(str): title(str)}"""
        try:
            return {m["movieId"]: m["title"] for m in self.movies_1k}
        except Exception:
            return {}


class Ratings:
    """
    Analyzing data from ratings.csv

    IMPORTANT for peer review:
    - joins ratings.csv + movies.csv inside constructor
    - stores join result (movie_titles) in field and uses it in methods
    """
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.rating_1k = []
        self.movies_path = os.path.join(os.path.dirname(file_path), "movies.csv")
        self.movie_titles = Movies(self.movies_path).get_movie_titles() if os.path.exists(self.movies_path) else {}

        for line in _read_first_1k(file_path, has_header=True):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                r = _safe_float(parts[2], None)
                ts = _safe_int(parts[3], None)
                if r is None or ts is None:
                    continue
                self.rating_1k.append({
                    "userId": parts[0],
                    "movieId": parts[1],
                    "rating": r,
                    "timestamp": ts
                })

    class Movies:
        """
        Analysis from ratings of movies perspective
        """
        def __init__(self, ratings_instance):
            self.rating_1k = ratings_instance.rating_1k
            self.movie_titles = ratings_instance.movie_titles

        def dist_by_year(self) -> dict:
            """{year(int): count(int)} sorted by year asc."""
            try:
                years = [
                    datetime.fromtimestamp(r["timestamp"]).year
                    for r in self.rating_1k
                    if r.get("timestamp") is not None
                ]
                c = collections.Counter(years)
                return dict(sorted(c.items(), key=lambda x: x[0]))
            except Exception:
                return {}

        def dist_by_rating(self) -> dict:
            """{rating(float): count(int)} sorted by rating asc."""
            try:
                vals = [
                    r["rating"]
                    for r in self.rating_1k
                    if isinstance(r.get("rating"), (int, float))
                ]
                c = collections.Counter(vals)
                return dict(sorted(c.items(), key=lambda x: x[0]))
            except Exception:
                return {}

        def top_by_num_of_ratings(self, n: int) -> dict:
            """{title(str): count(int)} sorted by count desc."""
            try:
                ids = [r["movieId"] for r in self.rating_1k if r.get("movieId") is not None]
                c = collections.Counter(ids)
                top = sorted(c.items(), key=lambda x: x[1], reverse=True)[:n]
                out = {}
                for mid, cnt in top:
                    out[self.movie_titles.get(mid, "Unknown")] = cnt
                return out
            except Exception:
                return {}

        def top_by_ratings(self, n: int, metric: str = "average") -> dict:
            """
            {title(str): metric(float)} sorted by metric desc, rounded to 2 decimals.
            metric: average(default) or median
            """
            try:
                mr = collections.defaultdict(list) -> { 'movieId': [rating], 'movieId}
                for r in self.rating_1k:
                    mid = r.get("movieId")
                    val = r.get("rating")
                    if mid is None or not isinstance(val, (int, float)):
                        continue
                    mr[mid].append(val)

                scored = {}
                for mid, values in mr.items():
                    if not values:
                        continue

                    if metric == "median":
                        s = sorted(values)
                        m = len(s)
                        val = s[m // 2] if m % 2 else (s[m // 2 - 1] + s[m // 2]) / 2
                    else:
                        val = sum(values) / len(values)

                    scored[self.movie_titles.get(mid, "Unknown")] = round(float(val), 2) 

                top = sorted(scored.items(), key=lambda x: x[1], reverse=True)[:n]
                return dict(top)
            except Exception:
                return {}

        def top_controversial(self, n: int) -> dict:
            """{title(str): variance(float)} sorted desc, rounded to 2 decimals."""
            try:
                mr = collections.defaultdict(list)
                for r in self.rating_1k:
                    mid = r.get("movieId")
                    val = r.get("rating")
                    if mid is None or not isinstance(val, (int, float)):
                        continue
                    mr[mid].append(val)

                vars_ = {}
                for mid, values in mr.items():
                    if len(values) < 2:
                        var = 0.0
                    else:
                        mean = sum(values) / len(values)
                        var = sum((x - mean) ** 2 for x in values) / len(values)

                    vars_[self.movie_titles.get(mid, "Unknown")] = round(float(var), 2)

                top = sorted(vars_.items(), key=lambda x: x[1], reverse=True)[:n]
                return dict(top)
            except Exception:
                return {}

        # -----------------------------
        # BONUS METHODS
        # -----------------------------

        def top_by_num_of_ratings_threshold(self, min_ratings: int) -> dict:
            """
            Returns {title: count} for movies with count >= min_ratings.
            Sorted by count descendingly.
            """
            try:
                movie_counts = collections.Counter(
                    r["movieId"] for r in self.rating_1k if r.get("movieId") is not None
                )
                filtered = {}
                for mid, cnt in movie_counts.items():
                    if cnt >= min_ratings:
                        filtered[self.movie_titles.get(mid, "Unknown")] = cnt
                return dict(sorted(filtered.items(), key=lambda x: x[1], reverse=True))
            except Exception:
                return {}

        def top_by_ratings_filtered(self, n: int, metric: str = "average", min_ratings: int = 5) -> dict:
            """
            Top-n movies by rating metric (average/median),
            considering only movies with at least min_ratings ratings.
            Sorted by metric descendingly, values rounded to 2 decimals.
            """
            try:
                mr = collections.defaultdict(list)
                for r in self.rating_1k:
                    mid = r.get("movieId")
                    val = r.get("rating")
                    if mid is None or not isinstance(val, (int, float)):
                        continue
                    mr[mid].append(val)

                scored = {}
                for mid, values in mr.items():
                    if len(values) < min_ratings:
                        continue

                    if metric == "median":
                        s = sorted(values)
                        m = len(s)
                        val = s[m // 2] if m % 2 else (s[m // 2 - 1] + s[m // 2]) / 2
                    else:
                        val = sum(values) / len(values)

                    scored[self.movie_titles.get(mid, "Unknown")] = round(float(val), 2)

                top = sorted(scored.items(), key=lambda x: x[1], reverse=True)[:n]
                return dict(top)
            except Exception:
                return {}

    class Users(Movies):
        """
        Analysis from ratings of users perspective.
        MUST inherit from Movies (peer checklist).
        """
        def __init__(self, ratings_instance):
            self.rating_1k = ratings_instance.rating_1k
            self.movie_titles = ratings_instance.movie_titles

        def dist_by_rating_number(self) -> dict:
            """{num_ratings(int): num_users(int)} sorted by num_ratings asc."""
            try:
                user_counts = collections.Counter(
                    [r["userId"] for r in self.rating_1k if r.get("userId") is not None]
                )
                dist = collections.Counter(user_counts.values())
                return dict(sorted(dist.items(), key=lambda x: x[0]))
            except Exception:
                return {}

        def dist_by_rating_values(self, n: int, metric: str = "average") -> dict:
            """
            {metric_value(float): users_count(int)} sorted by metric_value desc, take top-n metric values.
            metric: average(default) or median
            """
            try:
                ur = collections.defaultdict(list)
                for r in self.rating_1k:
                    uid = r.get("userId")
                    val = r.get("rating")
                    if uid is None or not isinstance(val, (int, float)):
                        continue
                    ur[uid].append(val)

                metrics = {}
                for uid, vals in ur.items():
                    if not vals:
                        continue
                    if metric == "median":
                        s = sorted(vals)
                        m = len(s)
                        v = s[m // 2] if m % 2 else (s[m // 2 - 1] + s[m // 2]) / 2
                    else:
                        v = sum(vals) / len(vals)
                    metrics[uid] = round(float(v), 2)

                dist = {}
                for mv in metrics.values():
                    dist[mv] = dist.get(mv, 0) + 1

                sorted_dist = dict(sorted(dist.items(), key=lambda x: x[0], reverse=True))
                return dict(list(sorted_dist.items())[:n])
            except Exception:
                return {}

        def top_by_variance(self, n: int) -> dict:
            """{userId(str): variance(float)} sorted desc, rounded to 2 decimals."""
            try:
                ur = collections.defaultdict(list)
                for r in self.rating_1k:
                    uid = r.get("userId")
                    val = r.get("rating")
                    if uid is None or not isinstance(val, (int, float)):
                        continue
                    ur[uid].append(val)

                vars_ = {}
                for uid, vals in ur.items():
                    if len(vals) < 2:
                        var = 0.0
                    else:
                        mean = sum(vals) / len(vals)
                        var = sum((x - mean) ** 2 for x in vals) / len(vals)
                    vars_[uid] = round(float(var), 2)

                top = sorted(vars_.items(), key=lambda x: x[1], reverse=True)[:n]
                return dict(top)
            except Exception:
                return {}


class Tags:
    """
    Analyzing data from tags.csv
    """
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.tags_1k = []

        for line in _read_first_1k(file_path, has_header=True):
            parts = _parse_csv_line_relaxed(line)
            if len(parts) >= 4:
                self.tags_1k.append({
                    "userId": parts[0],
                    "movieId": parts[1],
                    "tag": parts[2].strip(),
                    "timestamp": parts[3]
                })

    def most_words(self, n: int) -> dict:
        """{tag: word_count} top-n by word_count desc (drop duplicates)."""
        try:
            uniq = set(t["tag"] for t in self.tags_1k if t.get("tag") is not None)
            wc = {tag: len([w for w in tag.split() if w]) for tag in uniq}
            top = sorted(wc.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)
        except Exception:
            return {}

    def longest(self, n: int) -> list:
        """list[str] top-n longest tags by len desc (drop duplicates)."""
        try:
            uniq = list(set(t["tag"] for t in self.tags_1k if t.get("tag") is not None))
            return sorted(uniq, key=len, reverse=True)[:n]
        except Exception:
            return []

    def most_words_and_longest(self, n: int) -> list:
        """intersection of top-n most_words and top-n longest."""
        try:
            a = set(self.most_words(n).keys())
            b = set(self.longest(n))
            return list(a.intersection(b))
        except Exception:
            return []

    def most_popular(self, n: int) -> dict:
        """
        {tag: count} sorted desc.
        Uses "unique tag per movie" counting.
        """
        try:
            seen = {}
            c = collections.Counter()
            for t in self.tags_1k:
                mid = t.get("movieId")
                tag = t.get("tag")
                if mid is None or tag is None:
                    continue
                if mid not in seen:
                    seen[mid] = set()
                if tag not in seen[mid]:
                    seen[mid].add(tag)
                    c[tag] += 1
            top = sorted(c.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)
        except Exception:
            return {}

    def tags_with(self, word: str) -> list:
        """list[str] unique tags containing word (case-insensitive), sorted alphabetically."""
        try:
            w = (word or "").lower()
            out = set()
            for t in self.tags_1k:
                tag = t.get("tag", "")
                if w and w in tag.lower():
                    out.add(tag)
            return sorted(list(out))
        except Exception:
            return []

    # -----------------------------
    # BONUS METHOD
    # -----------------------------

    def most_popular_by_genre(self, movies: Movies, n: int, genre: str) -> dict:
        """
        Returns most popular tags for movies of a specific genre.
        {tag(str): count(int)} sorted by count desc, top-n.
        """
        try:
            genre_movie_ids = {
                m["movieId"]
                for m in movies.movies_1k
                if genre in m.get("genres", "")
            }
            c = collections.Counter()
            for t in self.tags_1k:
                if t.get("movieId") in genre_movie_ids:
                    tag = t.get("tag")
                    if tag is not None:
                        c[tag] += 1
            return dict(c.most_common(n))
        except Exception:
            return {}


class Links:
    """
    Analyzing data from links.csv

    IMPORTANT for peer review:
    - joins links.csv + movies.csv inside constructor
    - stores join result (movie_titles) in field and uses it in methods
    """
    def __init__(self, path_to_the_file: str) -> None:
        self.file_path = path_to_the_file
        self.links = []
        self.imdb_data = {}

        self.movies_path = os.path.join(os.path.dirname(path_to_the_file), "movies.csv")
        self.movie_titles = Movies(self.movies_path).get_movie_titles() if os.path.exists(self.movies_path) else {}

        for line in _read_first_1k(path_to_the_file, has_header=True):
            parts = line.strip().split(",")
            if len(parts) >= 2:
                self.links.append({
                    "movieId": parts[0],
                    "imdbId": parts[1],
                    "tmdbId": parts[2] if len(parts) > 2 else None
                })

    def _get_movie_title(self, movie_id: str) -> str:
        try:
            return self.movie_titles.get(str(movie_id), "Unknown")
        except Exception:
            return "Unknown"

    def get_imdb(self, list_of_movies: list, list_of_fields: list) -> list:
        """
        Returns list of lists: [movieId, field1, field2, ...] sorted by movieId desc.
        Handles request failures safely.
        """
        try:
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "en-US,en;q=0.5"
            }
            out = []

            for movie_id in list_of_movies:
                imdb_id = None
                for link in self.links:
                    if link.get("movieId") == movie_id:
                        imdb_id = link.get("imdbId")
                        break

                if not imdb_id:
                    out.append([movie_id] + ["N/A"] * len(list_of_fields))
                    continue

                imdb_id_formatted = str(imdb_id).zfill(7)
                url = f"https://www.imdb.com/title/tt{imdb_id_formatted}/"

                row = [movie_id]
                try:
                    if movie_id in self.imdb_data:
                        cached = self.imdb_data[movie_id]
                        for field in list_of_fields:
                            row.append(cached.get(field, "N/A"))
                    else:
                        resp = requests.get(url, headers=headers, timeout=10)
                        if resp.status_code != 200:
                            row.extend(["N/A"] * len(list_of_fields))
                        else:
                            soup = BeautifulSoup(resp.text, "html.parser")
                            movie_data = {}
                            for f in ["Director", "Budget", "Cumulative Worldwide Gross", "Runtime"]:
                                movie_data[f] = self._parse_imdb_field(soup, f)
                            self.imdb_data[movie_id] = movie_data
                            for field in list_of_fields:
                                row.append(movie_data.get(field, "N/A"))
                except Exception:
                    row.extend(["N/A"] * len(list_of_fields))

                out.append(row)

            return sorted(out, key=lambda x: _safe_int(x[0], -1), reverse=True)
        except Exception:
            return []

    def _parse_imdb_field(self, soup, field: str) -> str:
        """Parse minimal fields from IMDB page; return 'N/A' if not found."""
        try:
            if field == "Director":
                a = soup.find("a", href=re.compile(r"/name/nm"))
                return a.get_text(strip=True) if a else "N/A"

            if field == "Budget":
                text = soup.get_text(" ", strip=True)
                m = re.search(r"Budget\s*\$([\d,]+)", text, re.IGNORECASE)
                return f"${m.group(1)}" if m else "N/A"

            if field == "Cumulative Worldwide Gross":
                text = soup.get_text(" ", strip=True)
                m = re.search(r"(Cumulative Worldwide Gross|Gross worldwide)\s*\$([\d,]+)", text, re.IGNORECASE)
                return f"${m.group(2)}" if m else "N/A"

            if field == "Runtime":
                text = soup.get_text(" ", strip=True)
                m = re.search(r"(\d+)\s*min", text)
                if m:
                    return f"{m.group(1)} min"
                m = re.search(r"(\d+)\s*h\s*(\d+)\s*m", text)
                if m:
                    return f"{m.group(1)}h {m.group(2)}m"
                return "N/A"

            return "N/A"
        except Exception:
            return "N/A"

    def top_directors(self, n: int) -> dict:
        """{director: count} sorted desc."""
        try:
            ids = [l["movieId"] for l in self.links[:100]]
            rows = self.get_imdb(ids, ["Director"])
            c = collections.Counter()
            for r in rows:
                if len(r) > 1 and r[1] != "N/A":
                    c[r[1]] += 1
            top = sorted(c.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)
        except Exception:
            return {}

    def most_expensive(self, n: int) -> dict:
        """{title: budget(float)} sorted desc."""
        try:
            ids = [l["movieId"] for l in self.links[:100]]
            rows = self.get_imdb(ids, ["Budget"])
            d = {}
            for r in rows:
                if len(r) > 1:
                    b = _extract_number(r[1])
                    if b > 0:
                        d[self._get_movie_title(r[0])] = b
            top = sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)
        except Exception:
            return {}

    def most_profitable(self, n: int) -> dict:
        """{title: (gross-budget)(float)} sorted desc."""
        try:
            ids = [l["movieId"] for l in self.links[:100]]
            rows = self.get_imdb(ids, ["Budget", "Cumulative Worldwide Gross"])
            d = {}
            for r in rows:
                if len(r) > 2:
                    b = _extract_number(r[1])
                    g = _extract_number(r[2])
                    p = g - b
                    if p > 0:
                        d[self._get_movie_title(r[0])] = p
            top = sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)
        except Exception:
            return {}

    def longest(self, n: int) -> dict:
        """{title: runtime_minutes(int)} sorted desc."""
        try:
            ids = [l["movieId"] for l in self.links[:100]]
            rows = self.get_imdb(ids, ["Runtime"])
            d = {}
            for r in rows:
                if len(r) > 1:
                    m = _runtime_to_minutes(r[1])
                    if m > 0:
                        d[self._get_movie_title(r[0])] = m
            top = sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)
        except Exception:
            return {}

    def top_cost_per_minute(self, n: int) -> dict:
        """{title: cost_per_minute(float)} rounded 2, sorted desc."""
        try:
            ids = [l["movieId"] for l in self.links[:100]]
            rows = self.get_imdb(ids, ["Budget", "Runtime"])
            d = {}
            for r in rows:
                if len(r) > 2:
                    b = _extract_number(r[1])
                    m = _runtime_to_minutes(r[2])
                    if b > 0 and m > 0:
                        d[self._get_movie_title(r[0])] = round(b / m, 2)
            top = sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]
            return dict(top)
        except Exception:
            return {}


# -----------------------------
# Tests (PyTest)
# -----------------------------

class Tests:
    """
    Tests for each method of classes:
    - correct return types
    - correct element types
    - correct sorting
    - cover all public methods (incl. bonus)
    """

    @staticmethod
    @pytest.fixture
    def path_to_datasets():
        return "../datasets/ml-latest-small/"

    #Movies tests

    @staticmethod
    def test_movies_dist_by_release(path_to_datasets):
        m = Movies(os.path.join(path_to_datasets, "movies.csv"))
        res = m.dist_by_release()
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, str) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            vals = list(res.values())
            assert vals == sorted(vals, reverse=True)

    @staticmethod
    def test_movies_dist_by_genres(path_to_datasets):
        m = Movies(os.path.join(path_to_datasets, "movies.csv"))
        res = m.dist_by_genres()
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, str) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            vals = list(res.values())
            assert vals == sorted(vals, reverse=True)

    @staticmethod
    def test_movies_most_genres(path_to_datasets):
        m = Movies(os.path.join(path_to_datasets, "movies.csv"))
        res = m.most_genres(5)
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, str) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            vals = list(res.values())
            assert vals == sorted(vals, reverse=True)

    @staticmethod
    def test_movies_get_movie_titles(path_to_datasets):
        m = Movies(os.path.join(path_to_datasets, "movies.csv"))
        res = m.get_movie_titles()
        assert isinstance(res, dict)
        if res:
            items = list(res.items())
            assert all(isinstance(k, str) for k, _ in items)
            assert all(isinstance(v, str) for _, v in items)

    #Ratings + join tests

    @staticmethod
    def test_ratings_join_in_constructor(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        assert isinstance(r.movie_titles, dict)
        assert r.movie_titles is not None

    #Ratings.Movies tests

    @staticmethod
    def test_ratings_movies_dist_by_year(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        rm = Ratings.Movies(r)
        res = rm.dist_by_year()
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, int) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            keys = list(res.keys())
            assert keys == sorted(keys)

    @staticmethod
    def test_ratings_movies_dist_by_rating(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        rm = Ratings.Movies(r)
        res = rm.dist_by_rating()
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, float) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            keys = list(res.keys())
            assert keys == sorted(keys)

    @staticmethod
    def test_ratings_movies_top_by_num_of_ratings(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        rm = Ratings.Movies(r)
        res = rm.top_by_num_of_ratings(10)
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, str) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            vals = list(res.values())
            assert vals == sorted(vals, reverse=True)

    @staticmethod
    def test_ratings_movies_top_by_ratings(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        rm = Ratings.Movies(r)

        res_avg = rm.top_by_ratings(10, metric="average")
        assert isinstance(res_avg, dict)
        if res_avg:
            assert all(isinstance(k, str) for k in res_avg.keys())
            assert all(isinstance(v, float) for v in res_avg.values())
            assert list(res_avg.values()) == sorted(list(res_avg.values()), reverse=True)
            for v in res_avg.values():
                assert v == round(v, 2)

        res_med = rm.top_by_ratings(10, metric="median")
        assert isinstance(res_med, dict)

    @staticmethod
    def test_ratings_movies_top_controversial(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        rm = Ratings.Movies(r)
        res = rm.top_controversial(10)
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, str) for k in res.keys())
            assert all(isinstance(v, float) for v in res.values())
            vals = list(res.values())
            assert vals == sorted(vals, reverse=True)
            for v in res.values():
                assert v == round(v, 2)

    #BONUS tests for Ratings.Movies

    @staticmethod
    def test_bonus_top_by_num_of_ratings_threshold(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        rm = Ratings.Movies(r)
        res = rm.top_by_num_of_ratings_threshold(5)
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, str) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            vals = list(res.values())
            assert vals == sorted(vals, reverse=True)

    @staticmethod
    def test_bonus_top_by_ratings_filtered(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        rm = Ratings.Movies(r)
        res = rm.top_by_ratings_filtered(5, metric="average", min_ratings=5)
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, str) for k in res.keys())
            assert all(isinstance(v, float) for v in res.values())
            vals = list(res.values())
            assert vals == sorted(vals, reverse=True)
            for v in res.values():
                assert v == round(v, 2)

    #Ratings.Users tests

    @staticmethod
    def test_ratings_users_inheritance(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        ru = Ratings.Users(r)
        assert isinstance(ru, Ratings.Movies)

    @staticmethod
    def test_ratings_users_dist_by_rating_number(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        ru = Ratings.Users(r)
        res = ru.dist_by_rating_number()
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, int) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            keys = list(res.keys())
            assert keys == sorted(keys)

    @staticmethod
    def test_ratings_users_dist_by_rating_values(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        ru = Ratings.Users(r)
        res = ru.dist_by_rating_values(10, metric="average")
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, float) for k in res.keys())
            assert all(isinstance(v, int) for v in res.values())
            keys = list(res.keys())
            assert keys == sorted(keys, reverse=True)

    @staticmethod
    def test_ratings_users_top_by_variance(path_to_datasets):
        r = Ratings(os.path.join(path_to_datasets, "ratings.csv"))
        ru = Ratings.Users(r)
        res = ru.top_by_variance(10)
        assert isinstance(res, dict)
        if res:
            assert all(isinstance(k, str) for k in res.keys())
            assert all(isinstance(v, float) for v in res.values())
            vals = list(res.values())
            assert vals == sorted(vals, reverse=True)
            for v in res.values():
                assert v == round(v, 2)

    # Tags tests (incl. bonus)

    @staticmethod
    def test_tags_all_methods(path_to_datasets):
        t = Tags(os.path.join(path_to_datasets, "tags.csv"))

        mw = t.most_words(5)
        assert isinstance(mw, dict)
        if mw:
            assert all(isinstance(k, str) for k in mw.keys())
            assert all(isinstance(v, int) for v in mw.values())
            assert list(mw.values()) == sorted(list(mw.values()), reverse=True)

        lg = t.longest(5)
        assert isinstance(lg, list)
        if lg:
            assert all(isinstance(x, str) for x in lg)
            lens = [len(x) for x in lg]
            assert lens == sorted(lens, reverse=True)

        mwl = t.most_words_and_longest(10)
        assert isinstance(mwl, list)
        if mwl:
            assert all(isinstance(x, str) for x in mwl)

        mp = t.most_popular(5)
        assert isinstance(mp, dict)
        if mp:
            assert all(isinstance(k, str) for k in mp.keys())
            assert all(isinstance(v, int) for v in mp.values())
            assert list(mp.values()) == sorted(list(mp.values()), reverse=True)

        tw = t.tags_with("sci")
        assert isinstance(tw, list)
        if tw:
            assert all(isinstance(x, str) for x in tw)
            assert tw == sorted(tw)

        # BONUS: tags by genre
        movies = Movies(os.path.join(path_to_datasets, "movies.csv"))
        bg = t.most_popular_by_genre(movies, 5, "Comedy")
        assert isinstance(bg, dict)
        if bg:
            assert all(isinstance(k, str) for k in bg.keys())
            assert all(isinstance(v, int) for v in bg.values())
            assert list(bg.values()) == sorted(list(bg.values()), reverse=True)


    @staticmethod
    def test_links_join_and_methods(path_to_datasets, monkeypatch):
        l = Links(os.path.join(path_to_datasets, "links.csv"))
        assert isinstance(l.movie_titles, dict)

        def fake_get_imdb(movie_ids, fields):
            rows = []
            for mid in movie_ids:
                row = [mid]
                for f in fields:
                    if f == "Director":
                        row.append("Mock Director")
                    elif f == "Budget":
                        row.append("$1,000,000")
                    elif f == "Cumulative Worldwide Gross":
                        row.append("$5,000,000")
                    elif f == "Runtime":
                        row.append("120 min")
                    else:
                        row.append("N/A")
                rows.append(row)
            return sorted(rows, key=lambda x: _safe_int(x[0], -1), reverse=True)

        monkeypatch.setattr(l, "get_imdb", fake_get_imdb)

        td = l.top_directors(3)
        assert isinstance(td, dict)
        if td:
            assert all(isinstance(k, str) for k in td.keys())
            assert all(isinstance(v, int) for v in td.values())
            assert list(td.values()) == sorted(list(td.values()), reverse=True)

        me = l.most_expensive(3)
        assert isinstance(me, dict)
        if me:
            assert all(isinstance(k, str) for k in me.keys())
            assert all(isinstance(v, float) for v in me.values())
            assert list(me.values()) == sorted(list(me.values()), reverse=True)

        mp = l.most_profitable(3)
        assert isinstance(mp, dict)
        if mp:
            assert all(isinstance(k, str) for k in mp.keys())
            assert all(isinstance(v, float) for v in mp.values())
            assert list(mp.values()) == sorted(list(mp.values()), reverse=True)

        lg = l.longest(3)
        assert isinstance(lg, dict)
        if lg:
            assert all(isinstance(k, str) for k in lg.keys())
            assert all(isinstance(v, int) for v in lg.values())
            assert list(lg.values()) == sorted(list(lg.values()), reverse=True)

        cpm = l.top_cost_per_minute(3)
        assert isinstance(cpm, dict)
        if cpm:
            assert all(isinstance(k, str) for k in cpm.keys())
            assert all(isinstance(v, float) for v in cpm.values())
            assert list(cpm.values()) == sorted(list(cpm.values()), reverse=True)
            for v in cpm.values():
                assert v == round(v, 2)

    @staticmethod
    def test_links_get_imdb_structure_and_sorting(path_to_datasets, monkeypatch):
        l = Links(os.path.join(path_to_datasets, "links.csv"))

        def fake_get_imdb(movie_ids, fields):
            rows = []
            for mid in movie_ids:
                rows.append([mid] + ["N/A"] * len(fields))
            return sorted(rows, key=lambda x: _safe_int(x[0], -1), reverse=True)

        monkeypatch.setattr(l, "get_imdb", fake_get_imdb)

        res = l.get_imdb(["1", "2", "3"], ["Director", "Runtime"])
        assert isinstance(res, list)
        for row in res:
            assert isinstance(row, list)
            assert len(row) == 3
            assert isinstance(row[0], str)

        ids = [_safe_int(r[0], -1) for r in res]
        assert ids == sorted(ids, reverse=True)


if __name__ == "__main__":
    pass
