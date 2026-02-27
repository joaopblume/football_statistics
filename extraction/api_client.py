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
        per_page_delay_seconds: float = 10.0,
        max_players_page: int = 3,
        rate_limit_retry_seconds: int = 10,
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
        self.max_players_page = max_players_page
        self.rate_limit_retry_seconds = rate_limit_retry_seconds

    def request(self, endpoint: str, params: Dict[str, Any], retries: int = 2) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        attempt = 0
        while True:
            try:
                resp = requests.get(url, headers=self.headers, params=params, timeout=30)

                if resp.status_code == 429 and retries > 0:
                    retry_after = int(resp.headers.get("Retry-After", str(self.rate_limit_retry_seconds)))
                    print(f"[WARN] Rate limit HTTP 429 em {endpoint}. Aguardando {retry_after}s para tentar novamente.")
                    time.sleep(retry_after)
                    retries -= 1
                    continue

                if resp.status_code >= 400:
                    print(f"[ERRO] HTTP {resp.status_code} endpoint={endpoint} params={params}")
                    print(f"[ERRO] Body: {resp.text[:400]}")
                    resp.raise_for_status()

                data = resp.json()
                errors = data.get("errors") or {}
                if errors.get("rateLimit"):
                    wait_seconds = self.rate_limit_retry_seconds
                    attempt += 1
                    print(
                        f"[WARN] Rate limit da API ({errors.get('rateLimit')}). "
                        f"Tentativa {attempt}. Aguardando {wait_seconds}s para repetir requisicao."
                    )
                    time.sleep(wait_seconds)
                    continue

                if not data.get("response"):
                    print(
                        f"[AVISO] Resposta vazia endpoint={endpoint} params={params} "
                        f"results={data.get('results')} errors={errors} paging={data.get('paging')}"
                    )
                return data
            except requests.RequestException as exc:
                if retries > 0:
                    retries -= 1
                    print(
                        f"[WARN] Erro de rede endpoint={endpoint} params={params}: {exc}. "
                        f"Aguardando {self.rate_limit_retry_seconds}s para tentar novamente."
                    )
                    time.sleep(self.rate_limit_retry_seconds)
                    continue
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
            if page > self.max_players_page:
                print(
                    f"[AVISO] Limite de pagina configurado atingido para players: "
                    f"team_id={team_id} max_page={self.max_players_page}."
                )
                break

            data = self.get_players_by_team(team_id=team_id, season=season, page=page)
            errors = data.get("errors") or {}
            if errors.get("plan"):
                print(f"[AVISO] Restricao do plano ao consultar players: {errors.get('plan')}")
                break

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
