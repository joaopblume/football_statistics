import os
import time
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv


class ApiFootballClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://v3.football.api-sports.io",
        per_page_delay_seconds: float = 1.0,
    ):
        load_dotenv()
        self.api_key = api_key or os.getenv("API_FOOTBALL_KEY")
        if not self.api_key:
            raise ValueError(
                "API key nao fornecida. Defina API_FOOTBALL_KEY no ambiente ou passe como parametro."
            )

        self.base_url = base_url.rstrip("/")
        self.headers = {"x-apisports-key": self.api_key}
        self.per_page_delay_seconds = per_page_delay_seconds

    def request(self, endpoint: str, params: Dict[str, Any], retries: int = 2) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            resp = requests.get(url, headers=self.headers, params=params, timeout=30)

            if resp.status_code == 429 and retries > 0:
                retry_after = int(resp.headers.get("Retry-After", "60"))
                print(f"[WARN] Rate limit em {endpoint}. Aguardando {retry_after}s para tentar novamente.")
                time.sleep(retry_after)
                return self.request(endpoint=endpoint, params=params, retries=retries - 1)

            if resp.status_code >= 400:
                print(f"[ERRO] HTTP {resp.status_code} endpoint={endpoint} params={params}")
                print(f"[ERRO] Body: {resp.text[:400]}")
                resp.raise_for_status()

            data = resp.json()
            if not data.get("response"):
                print(
                    f"[AVISO] Resposta vazia endpoint={endpoint} params={params} "
                    f"results={data.get('results')} errors={data.get('errors')} paging={data.get('paging')}"
                )
            return data
        except requests.RequestException as exc:
            return {"response": [], "errors": {"request_exception": str(exc)}}

    def get_league(self, league_id: int, season: int) -> Dict[str, Any]:
        return self.request("leagues", {"id": league_id, "season": season})

    def get_teams(self, league_id: int, season: int) -> Dict[str, Any]:
        return self.request("teams", {"league": league_id, "season": season})

    def get_players_by_team(self, team_id: int, season: int, page: int = 1) -> Dict[str, Any]:
        return self.request("players", {"team": team_id, "season": season, "page": page})

    def get_all_players_by_team(self, team_id: int, season: int) -> List[Dict[str, Any]]:
        players: List[Dict[str, Any]] = []
        page = 1

        while True:
            data = self.get_players_by_team(team_id=team_id, season=season, page=page)
            response = data.get("response") or []
            players.extend(response)

            paging = data.get("paging") or {}
            current = int(paging.get("current") or page)
            total = int(paging.get("total") or current)

            if current >= total:
                break
            page += 1
            if self.per_page_delay_seconds > 0:
                time.sleep(self.per_page_delay_seconds)

        return players
