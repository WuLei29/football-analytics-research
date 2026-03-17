import json
import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import psycopg2
import psycopg2.extras


class FootballEventsProcessor:
    """
    Procesa archivos de eventos de fútbol JSONP y los convierte en DataFrames estructurados.
    Incluye cálculo de carries entre eventos consecutivos y Expected Threat (xT).

    Attributes:
        event_codes (dict): Mapeo de typeId a información del evento
        qualifier_codes (dict): Mapeo de qualifierId a información del qualifier
        qualifier_columns (dict): Columnas dinámicas para qualifiers
        player_jersey_mapping (dict): Mapeo de source_player_id a jersey number
        team_mapping (dict): Mapeo de source_team_id a teamName / h_a / oppositionTeamName
    """

    # ------------------------------------------------------------------
    # xT grid: 8 rows (Y axis, 0-68) × 12 columns (X axis, 0-105).
    # Row 0 = bottom of pitch (y≈0), row 7 = top (y≈68).
    # Column 0 = own goal line (x=0), column 11 = opponent goal line (x=105).
    # ------------------------------------------------------------------
    XT_GRID = np.array([
        [0.00638303, 0.00779616, 0.00844854, 0.00977659, 0.01126267, 0.01248344, 0.01473596, 0.0174506,  0.02122129, 0.02756312, 0.03485072, 0.0379259 ],
        [0.00750072, 0.00878589, 0.00942382, 0.0105949,  0.01214719, 0.0138454,  0.01611813, 0.01870347, 0.02401521, 0.02953272, 0.04066992, 0.04647721],
        [0.0088799,  0.00977745, 0.01001304, 0.01110462, 0.01269174, 0.01429128, 0.01685596, 0.01935132, 0.0241224,  0.02855202, 0.05491138, 0.06442595],
        [0.00941056, 0.01082722, 0.01016549, 0.01132376, 0.01262646, 0.01484598, 0.01689528, 0.0199707,  0.02385149, 0.03511326, 0.10805102, 0.25745362],
        [0.00941056, 0.01082722, 0.01016549, 0.01132376, 0.01262646, 0.01484598, 0.01689528, 0.0199707,  0.02385149, 0.03511326, 0.10805102, 0.25745362],
        [0.0088799,  0.00977745, 0.01001304, 0.01110462, 0.01269174, 0.01429128, 0.01685596, 0.01935132, 0.0241224,  0.02855202, 0.05491138, 0.06442595],
        [0.00750072, 0.00878589, 0.00942382, 0.0105949,  0.01214719, 0.0138454,  0.01611813, 0.01870347, 0.02401521, 0.02953272, 0.04066992, 0.04647721],
        [0.00638303, 0.00779616, 0.00844854, 0.00977659, 0.01126267, 0.01248344, 0.01473596, 0.0174506,  0.02122129, 0.02756312, 0.03485072, 0.0379259 ],
    ])

    # Columns written to silver.events.
    # Columns written to silver.events — order and names must match the DDL exactly.
    # The 550-column qualifier explosion (type_value_* / value_*) is NOT
    # persisted — raw_data covers any future promotion needs.
    # Sequence columns are included here as NULL placeholders; they are
    # populated in a separate pass by classify_possession_sequences().
    SILVER_COLUMNS = [
        # Identifiers
        'source_event_id',
        'provider_event_id',
        'match_id',
        # Timing
        'period', 'minute', 'second', 'timestamp',
        # Participants — internal FKs + provider source IDs kept for cross-row joins
        'team_id', 'source_team_id',
        'player_id', 'source_player_id',
        'player_name', 'jersey_number',
        # Team context — denormalised for query convenience
        'team_name', 'opposition_team_name', 'h_a',
        # Event classification
        'type_id', 'event_type', 'outcome',
        # Coordinates — real pitch dimensions (X: 0-105, Y: 0-68)
        'x', 'y', 'end_x', 'end_y',
        'blocked_x', 'blocked_y',
        'goal_mouth_z', 'goal_mouth_y',
        # Threat metrics
        'start_zone_value_xt', 'end_zone_value_xt', 'xt',
        # Possession sequences — NULL on initial load, populated in second pass
        'sequence_id', 'sequence_start', 'sequence_end', 'sequence_event_number',
        # Safety net
        'raw_data',
    ]

    # Outcome mapping: raw provider integer → spec string
    OUTCOME_MAP = {1: 'success', 0: 'failure'}

    def __init__(self, events_mapping_path: str, qualifiers_mapping_path: str):
        """
        Inicializa el procesador con los archivos de mapeo.

        Args:
            events_mapping_path: Ruta al archivo de mapeo de eventos (OptaEvents)
            qualifiers_mapping_path: Ruta al archivo de mapeo de qualifiers
        """
        self.event_codes = self._load_mapping_file(events_mapping_path)
        self.qualifier_codes = self._load_mapping_file(qualifiers_mapping_path)

        self.qualifier_columns = self._create_qualifier_columns()
        self.player_jersey_mapping: Dict[str, int] = {}

        # {source_team_id: {'teamName': str, 'oppositionTeamName': str, 'h_a': str}}
        self.team_mapping: Dict[str, Dict[str, str]] = {}

        # Populated once per load_all_matches run from silver
        # {source_team_id: team_id (int)}
        self._team_id_cache: Dict[str, int] = {}
        # {source_player_id: player_id (int)}
        self._player_id_cache: Dict[str, int] = {}

        self.basic_columns_mapping = {
            'id':           'source_event_id',      # provider's unique event uuid
            'eventId':      'provider_event_id',    # provider's sequential event number
            'typeId':       'type_id',
            'timeMin':      'minute',
            'timeSec':      'second',
            'contestantId': 'source_team_id',
            'playerId':     'source_player_id',
            'playerName':   'player_name',
            'x':            'x',
            'y':            'y',
            'periodId':     'period',
            'timeStamp':    'timestamp',
            'outcome':      '_outcome_raw',         # temp; mapped to 'outcome' below
        }

    # -------------------------------------------------------------------------
    # MAPPING / LOADING
    # -------------------------------------------------------------------------

    def _load_mapping_file(self, file_path: str) -> Dict[int, Dict[str, str]]:
        """
        Carga archivo de mapeo JavaScript y extrae el diccionario.

        Args:
            file_path: Ruta al archivo de mapeo

        Returns:
            Diccionario con el mapeo {id: {name, description, value}}
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()

            pattern = r'var\s+\w+\s*=\s*(\{.*\});?'
            match = re.search(pattern, content, re.DOTALL)

            if match:
                js_object = match.group(1)
                js_object = re.sub(r'(\w+):', r'"\1":', js_object)
                js_object = re.sub(r"'([^']*)'", r'"\1"', js_object)

                mapping_dict = json.loads(js_object)
                return {int(k): v for k, v in mapping_dict.items()}
            else:
                raise ValueError(f"No se pudo extraer el mapeo de {file_path}")

        except Exception as e:
            print(f"Error cargando mapeo desde {file_path}: {e}")
            return {}

    def _create_qualifier_columns(self) -> Dict[str, List[str]]:
        """
        Crea las columnas dinámicas para todos los qualifiers posibles.

        Returns:
            Diccionario con información de las columnas de qualifiers
        """
        type_value_columns = []
        value_columns = []

        for qualifier_id, qualifier_info in self.qualifier_codes.items():
            qualifier_name = qualifier_info['name']
            type_value_columns.append(f"type_value_{qualifier_name}")
            value_columns.append(f"value_{qualifier_name}")

        return {
            'type_value': type_value_columns,
            'value': value_columns,
            'all': type_value_columns + value_columns
        }

    # -------------------------------------------------------------------------
    # JSON / JSONP PARSING
    # -------------------------------------------------------------------------

    def _extract_json_from_jsonp(self, file_path: str) -> Dict[str, Any]:
        """
        Extrae JSON de archivo JSONP eliminando el wrapper de función.

        Args:
            file_path: Ruta al archivo JSONP

        Returns:
            Diccionario con los datos JSON
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        pattern = r'^[^(]+\((.*)\)$'
        match = re.search(pattern, content, re.DOTALL)

        if match:
            json_str = match.group(1)
            return json.loads(json_str)
        else:
            raise ValueError("No se pudo extraer JSON del archivo JSONP")

    def _extract_source_match_id(self, file_path: str) -> Optional[str]:
        """
        Extrae el source_match_id (matchInfo.id) del archivo JSONP sin procesar
        el resto del contenido. Útil para lookups rápidos antes del procesado completo.

        Args:
            file_path: Ruta al archivo JSONP

        Returns:
            source_match_id como string, o None si no se encuentra
        """
        try:
            data = self._extract_json_from_jsonp(file_path)
            return data.get('matchInfo', {}).get('id')
        except Exception as e:
            print(f"Error extrayendo source_match_id de {file_path}: {e}")
            return None

    # -------------------------------------------------------------------------
    # SILVER FK RESOLUTION
    # -------------------------------------------------------------------------

    def _load_id_caches(self, conn) -> None:
        """
        Carga en memoria los mapeos source_id → internal_id desde silver.
        Se ejecuta una sola vez al inicio de load_all_matches.

        Carga:
            - silver.teams:   source_team_id   → team_id
            - silver.players: source_player_id → player_id
        """
        with conn.cursor() as cur:
            cur.execute("SELECT source_team_id, team_id FROM silver.teams")
            self._team_id_cache = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute("SELECT source_player_id, player_id FROM silver.players")
            self._player_id_cache = {row[0]: row[1] for row in cur.fetchall()}

        print(f"FK caches cargados: {len(self._team_id_cache)} equipos, "
              f"{len(self._player_id_cache)} jugadores")

    def _resolve_match_id(self, source_match_id: str, conn) -> Optional[int]:
        """
        Resuelve un source_match_id al match_id interno de silver.matches.

        Args:
            source_match_id: Provider's matchInfo.id string
            conn: psycopg2 connection

        Returns:
            Internal match_id int, o None si no existe en silver.matches
        """
        with conn.cursor() as cur:
            cur.execute(
                "SELECT match_id FROM silver.matches WHERE source_match_id = %s",
                (source_match_id,)
            )
            row = cur.fetchone()
        return row[0] if row else None

    def _check_match_already_loaded(self, match_id: int, conn) -> bool:
        """
        Comprueba si ya existen eventos en silver.events para este match_id.

        Args:
            match_id: Internal match_id
            conn: psycopg2 connection

        Returns:
            True si ya hay eventos cargados para este partido
        """
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM silver.events WHERE match_id = %s LIMIT 1",
                (match_id,)
            )
            return cur.fetchone() is not None

    def _resolve_fk_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Resuelve source_team_id → team_id y source_player_id → player_id
        usando los caches cargados desde silver.

        Las columnas source_* se conservan en el DataFrame para debugging
        pero no se escriben en silver.events.

        Args:
            df: DataFrame con columnas source_team_id y source_player_id

        Returns:
            DataFrame con columnas team_id y player_id añadidas
        """
        df['team_id'] = df['source_team_id'].map(self._team_id_cache)
        df['player_id'] = df['source_player_id'].map(self._player_id_cache)

        unresolved_teams = df.loc[df['team_id'].isna(), 'source_team_id'].unique()
        if len(unresolved_teams) > 0:
            print(f"source_team_id sin resolver: {list(unresolved_teams)}")

        # player_id is nullable — only warn for non-null source_player_ids that failed
        unresolved_players = df.loc[
            df['player_id'].isna() & df['source_player_id'].notna(),
            'source_player_id'
        ].unique()
        if len(unresolved_players) > 0:
            print(f"source_player_id sin resolver ({len(unresolved_players)} jugadores): "
                  f"{list(unresolved_players[:5])}{'...' if len(unresolved_players) > 5 else ''}")

        return df

    # -------------------------------------------------------------------------
    # JERSEY NUMBER MAPPING
    # -------------------------------------------------------------------------

    def _extract_player_jersey_mapping(self, events: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Extrae el mapeo de source_player_id a jersey number desde los eventos
        de Team Set Up (typeId: 34).

        Args:
            events: Lista de todos los eventos del archivo

        Returns:
            Diccionario con el mapeo {source_player_id: jerseyNumber}
        """
        player_jersey_map = {}

        setup_events = [event for event in events if event.get('typeId') == 34]

        if not setup_events:
            print("Advertencia: No se encontraron eventos de Team Set Up (typeId: 34)")
            return player_jersey_map

        for setup_event in setup_events:
            team_id = setup_event.get('contestantId')
            qualifiers = setup_event.get('qualifier', [])

            player_ids_str = None
            jersey_numbers_str = None

            for qualifier in qualifiers:
                qualifier_id = qualifier.get('qualifierId')
                if qualifier_id == 30:
                    player_ids_str = qualifier.get('value', '')
                elif qualifier_id == 59:
                    jersey_numbers_str = qualifier.get('value', '')

            if player_ids_str and jersey_numbers_str:
                player_ids = [pid.strip() for pid in player_ids_str.split(',')]
                jersey_numbers = [jn.strip() for jn in jersey_numbers_str.split(',')]

                if len(player_ids) != len(jersey_numbers):
                    print(f"Advertencia: Desajuste en cantidad de IDs ({len(player_ids)}) "
                          f"y números de camiseta ({len(jersey_numbers)}) para team {team_id}")
                    continue

                for player_id, jersey_num in zip(player_ids, jersey_numbers):
                    if player_id and jersey_num:
                        try:
                            player_jersey_map[player_id] = int(jersey_num)
                        except ValueError:
                            player_jersey_map[player_id] = 0

        print(f"Mapeo de jersey numbers creado: {len(player_jersey_map)} jugadores")
        return player_jersey_map

    # -------------------------------------------------------------------------
    # TEAM MAPPING
    # -------------------------------------------------------------------------

    def _extract_team_mapping(self, contestants: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
        """
        Construye el mapeo de equipos a partir de la lista 'contestant' del
        bloque matchInfo del archivo JSONP.

        Para cada equipo se almacena:
          - teamName          : nombre del club (campo 'name')
          - oppositionTeamName: nombre del rival
          - h_a               : 'home' o 'away' (campo 'position')

        Args:
            contestants: Lista de dicts con la información de cada equipo.

        Returns:
            Diccionario {source_team_id: {'teamName': str, 'oppositionTeamName': str, 'h_a': str}}
        """
        if not contestants or len(contestants) != 2:
            print(f"Advertencia: Se esperaban exactamente 2 equipos, "
                  f"se encontraron {len(contestants) if contestants else 0}")
            return {}

        team_mapping = {}

        for contestant in contestants:
            team_id   = contestant.get('id')
            team_name = contestant.get('name')
            h_a       = contestant.get('position')   # 'home' | 'away'

            if not team_id or not team_name:
                print(f"Advertencia: Datos de equipo incompletos: {contestant}")
                continue

            team_mapping[team_id] = {
                'teamName': team_name,
                'h_a':      h_a,
                'oppositionTeamName': None  # Se rellena en el segundo paso
            }

        # Rellenar oppositionTeamName cruzando los dos equipos
        if len(team_mapping) == 2:
            ids = list(team_mapping.keys())
            team_mapping[ids[0]]['oppositionTeamName'] = team_mapping[ids[1]]['teamName']
            team_mapping[ids[1]]['oppositionTeamName'] = team_mapping[ids[0]]['teamName']

        print(f"Mapeo de equipos creado: "
              + " vs ".join(f"{v['teamName']} ({v['h_a']})" for v in team_mapping.values()))

        return team_mapping

    # -------------------------------------------------------------------------
    # EVENT EXTRACTION
    # -------------------------------------------------------------------------

    def _extract_basic_columns(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrae las columnas básicas de un evento.

        Args:
            event: Diccionario con los datos del evento

        Returns:
            Diccionario con las columnas básicas mapeadas
        """
        row = {}

        for json_key, df_column in self.basic_columns_mapping.items():
            row[df_column] = event.get(json_key, None)

        # event_type: human name from typeId lookup
        type_id = event.get('typeId')
        if type_id is not None:
            event_info = self.get_event_info(type_id)
            row['event_type'] = event_info['name']
        else:
            row['event_type'] = None

        # outcome: map raw 0/1 integer to 'failure'/'success' string
        row['outcome'] = self.OUTCOME_MAP.get(row.pop('_outcome_raw'))

        # Placeholders for columns not yet implemented
        row['secondary_player_id'] = None
        row['xg'] = None

        # raw_data: full provider payload for future-proofing
        row['raw_data'] = json.dumps(event)

        # jersey_number (unified name)
        source_player_id = row.get('source_player_id')
        if source_player_id and source_player_id in self.player_jersey_mapping:
            row['jersey_number'] = self.player_jersey_mapping[source_player_id]
        elif source_player_id:
            print(f"source_player_id '{source_player_id}' no encontrado en el mapeo de jersey numbers")
            row['jersey_number'] = None
        else:
            row['jersey_number'] = None

        # Team columns: teamName, oppositionTeamName, h_a
        source_team_id = row.get('source_team_id')
        if source_team_id and source_team_id in self.team_mapping:
            team_info = self.team_mapping[source_team_id]
            row['team_name']            = team_info['teamName']
            row['opposition_team_name'] = team_info['oppositionTeamName']
            row['h_a']                  = team_info['h_a']
        else:
            row['team_name']            = None
            row['opposition_team_name'] = None
            row['h_a']                  = None

        # Initialise all qualifier columns to None
        for col_name in self.qualifier_columns['all']:
            row[col_name] = None

        self._process_event_qualifiers(event, row)
        self._create_special_qualifier_columns(row)
        self._apply_fallback_coordinates(row)

        return row

    def _process_event_qualifiers(self, event: Dict[str, Any], row: Dict[str, Any]) -> None:
        """
        Procesa los qualifiers de un evento específico y llena las columnas correspondientes.

        Args:
            event: Diccionario con los datos del evento
            row: Fila del DataFrame a llenar
        """
        qualifiers = event.get('qualifier', [])

        for qualifier in qualifiers:
            qualifier_id = qualifier.get('qualifierId')
            qualifier_value = qualifier.get('value')

            if qualifier_id is not None:
                qualifier_info = self.get_qualifier_info(qualifier_id)
                qualifier_name = qualifier_info['name']

                type_value_col = f"type_value_{qualifier_name}"
                value_col = f"value_{qualifier_name}"

                if type_value_col in row:
                    row[type_value_col] = qualifier_id
                if value_col in row:
                    row[value_col] = qualifier_value

    def _create_special_qualifier_columns(self, row: Dict[str, Any]) -> None:
        """
        Crea columnas especiales basadas en valores específicos de qualifiers.
        Convierte coordenadas de 0-100 a dimensiones reales del campo (105x68).

        Args:
            row: Fila del DataFrame a modificar
        """
        special_columns_mapping = {
            'end_x':      'value_Pass End X',
            'end_y':      'value_Pass End Y',
            'blocked_x':  'value_Blocked x co-ordinate',
            'blocked_y':  'value_Blocked y co-ordinate',
            'goal_mouth_z': 'value_Goal mouth z coordinate',
            'goal_mouth_y': 'value_Goal mouth y coordinate'
        }

        x_coordinate_columns = ['end_x', 'blocked_x', 'x']
        y_coordinate_columns = ['end_y', 'blocked_y', 'y']

        if 'x' in row and row['x'] is not None:
            try:
                row['x'] = (float(row['x']) / 100) * 105
            except (ValueError, TypeError):
                row['x'] = None

        if 'y' in row and row['y'] is not None:
            try:
                row['y'] = (float(row['y']) / 100) * 68
            except (ValueError, TypeError):
                row['y'] = None

        for special_col, qualifier_col in special_columns_mapping.items():
            row[special_col] = None
            if qualifier_col in row and row[qualifier_col] is not None:
                try:
                    value = float(row[qualifier_col])
                    if special_col in x_coordinate_columns:
                        row[special_col] = (value / 100) * 105
                    elif special_col in y_coordinate_columns:
                        row[special_col] = (value / 100) * 68
                    else:
                        row[special_col] = value
                except (ValueError, TypeError):
                    row[special_col] = None

    def _apply_fallback_coordinates(self, row: Dict[str, Any]) -> None:
        """
        Usa x/y como respaldo para end_x/end_y cuando estos son None.

        Args:
            row: Fila del DataFrame a modificar
        """
        if row['end_x'] is None and row['x'] is not None:
            row['end_x'] = float(row['x'])
        if row['end_y'] is None and row['y'] is not None:
            row['end_y'] = float(row['y'])

    # -------------------------------------------------------------------------
    # CARRY CALCULATION
    # -------------------------------------------------------------------------

    def _create_carry_row(
        self,
        event1: pd.Series,
        event2: pd.Series,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        source_player_id: Any,
        player_name: Optional[str],
        jersey_number: Optional[int],
        source_team_id: Any,
        period: Any
    ) -> Dict[str, Any]:
        """
        Crea una fila de evento carry con valores interpolados.

        Args:
            event1: Evento previo al carry
            event2: Evento posterior al carry
            start_x/y: Coordenadas de inicio del carry (en dimensiones reales 0-105 / 0-68)
            end_x/y: Coordenadas de fin del carry
            source_player_id: Provider ID del jugador que realiza el carry
            player_name: Nombre del jugador
            jersey_number: Número de camiseta
            source_team_id: Provider ID del equipo
            period: Período del partido

        Returns:
            Diccionario representando el evento carry
        """
        avg_minute = (event1['minute'] + event2['minute']) / 2
        avg_second = (event1['second'] + event2['second']) / 2

        if avg_second >= 60:
            avg_minute += 1
            avg_second -= 60

        team_info = self.team_mapping.get(source_team_id, {})

        return {
            # source_event_id is None for synthesized carries — no provider UUID
            'match_id':           getattr(self, '_current_match_id', None),
            'source_event_id':    None,
            'provider_event_id':  None,
            'type_id':            None,
            'minute':             int(avg_minute),
            'second':             avg_second,
            'source_team_id':     source_team_id,
            'team_name':          team_info.get('teamName'),
            'opposition_team_name': team_info.get('oppositionTeamName'),
            'h_a':                team_info.get('h_a'),
            'x':                  start_x,
            'y':                  start_y,
            'end_x':              end_x,
            'end_y':              end_y,
            'period':             period,
            'event_type':         'Carry',
            'source_player_id':   source_player_id,
            'player_name':        player_name,
            'jersey_number':      jersey_number,
            'outcome':            'success',
            'secondary_player_id': None,
            'xg':                 None,
            'raw_data':           None,
        }

    def calculate_carries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula e inserta eventos carry entre acciones consecutivas.

        Args:
            df: DataFrame de eventos ordenado por minuto y segundo

        Returns:
            DataFrame original con los eventos carry insertados
        """
        carries_to_insert = []

        for i in range(len(df) - 1):
            current_event = df.iloc[i]
            next_event = df.iloc[i + 1]

            current_type = current_event['event_type']
            next_type = next_event['event_type']
            current_outcome = current_event.get('outcome', None)
            next_outcome = next_event.get('outcome', None)

            same_team = current_event['source_team_id'] == next_event['source_team_id']

            # Check coordinate mismatch
            coords_dont_match = False
            if pd.notna(current_event.get('end_x')) and pd.notna(current_event.get('end_y')):
                if pd.notna(next_event.get('x')) and pd.notna(next_event.get('y')):
                    coords_dont_match = (
                        abs(current_event['end_x'] - next_event['x']) > 0.01 or
                        abs(current_event['end_y'] - next_event['y']) > 0.01
                    )

            create_carry = False
            carry_start_x = current_event.get('end_x')
            carry_start_y = current_event.get('end_y')
            carry_end_x = next_event.get('x')
            carry_end_y = next_event.get('y')

            # --- Edge cases: never create carry ---
            if next_type in ['BallTouch', 'BallRecovery', 'Aerial', 'CornerAwarded']:
                continue
            if current_type in ['Foul', 'Card']:
                continue
            if current_type == 'MissedShot' and next_type == 'BallTouch':
                continue

            # --- Tackle ---
            if current_type == 'Tackle':
                if next_type == 'BallRecovery' and current_event.get('source_player_id') != next_event.get('source_player_id'):
                    continue
                elif next_type == 'Pass' and current_event.get('source_player_id') == next_event.get('source_player_id') and same_team and coords_dont_match:
                    create_carry = True

            # --- Pass ---
            if current_type == 'Pass' and same_team and coords_dont_match and current_outcome == 'success':
                if next_type == 'Pass':
                    create_carry = True
                elif next_type in ['Shot', 'MissedShot', 'SavedShot', 'Dispossessed', 'Foul']:
                    create_carry = True

            # --- Ball recoveries / keeper / interceptions ---
            if current_type in ['BallRecovery', 'KeeperPickup', 'Interception', 'Claim']:
                if same_team and coords_dont_match and next_type in ['Pass', 'Shot', 'MissedShot', 'SavedShot']:
                    create_carry = True

            # --- Clearance ---
            if current_type == 'Clearance' and next_type == 'Pass' and same_team and coords_dont_match:
                create_carry = True

            # --- Before Dispossessed / Foul / SavedShot ---
            if next_type in ['Dispossessed', 'Foul', 'SavedShot']:
                if current_type == 'Pass' and same_team and coords_dont_match and current_outcome == 'success':
                    create_carry = True

            # --- Special: Challenge unsuccessful → TakeOn successful (different teams) ---
            if current_type == 'Challenge' and current_outcome == 'failure':
                if next_type == 'Take On' and next_outcome == 'success' and current_event['source_team_id'] != next_event['source_team_id']:
                    if i > 0:
                        prev_event = df.iloc[i - 1]
                        if (prev_event['source_team_id'] == next_event['source_team_id'] and
                                pd.notna(prev_event.get('end_x')) and pd.notna(prev_event.get('end_y')) and
                                pd.notna(next_event.get('x')) and pd.notna(next_event.get('y'))):
                            if (abs(prev_event['end_x'] - next_event['x']) > 0.01 or
                                    abs(prev_event['end_y'] - next_event['y']) > 0.01):
                                carries_to_insert.append((i + 1, self._create_carry_row(
                                    prev_event, next_event,
                                    prev_event['end_x'], prev_event['end_y'],
                                    next_event['x'], next_event['y'],
                                    next_event['source_player_id'], next_event.get('player_name'),
                                    next_event.get('jersey_number'),
                                    next_event['source_team_id'], next_event['period']
                                )))

                    if i + 2 < len(df):
                        next_next_event = df.iloc[i + 2]
                        if (next_next_event['source_team_id'] == next_event['source_team_id'] and
                                pd.notna(next_event.get('end_x')) and pd.notna(next_event.get('end_y')) and
                                pd.notna(next_next_event.get('x')) and pd.notna(next_next_event.get('y'))):
                            if (abs(next_event['end_x'] - next_next_event['x']) > 0.01 or
                                    abs(next_event['end_y'] - next_next_event['y']) > 0.01):
                                carries_to_insert.append((i + 2, self._create_carry_row(
                                    next_event, next_next_event,
                                    next_event['end_x'], next_event['end_y'],
                                    next_next_event['x'], next_next_event['y'],
                                    next_event['source_player_id'], next_event.get('player_name'),
                                    next_event.get('jersey_number'),
                                    next_event['source_team_id'], next_event['period']
                                )))
                    continue

            # --- Special: TakeOn successful → Challenge unsuccessful ---
            if current_type == 'Take On' and current_outcome == 'success':
                if next_type == 'Challenge' and next_outcome == 'failure' and current_event['source_team_id'] != next_event['source_team_id']:
                    if i > 0:
                        prev_event = df.iloc[i - 1]
                        if (prev_event['source_team_id'] == current_event['source_team_id'] and
                                pd.notna(prev_event.get('end_x')) and pd.notna(prev_event.get('end_y')) and
                                pd.notna(current_event.get('x')) and pd.notna(current_event.get('y'))):
                            if (abs(prev_event['end_x'] - current_event['x']) > 0.01 or
                                    abs(prev_event['end_y'] - current_event['y']) > 0.01):
                                carries_to_insert.append((i, self._create_carry_row(
                                    prev_event, current_event,
                                    prev_event['end_x'], prev_event['end_y'],
                                    current_event['x'], current_event['y'],
                                    current_event['source_player_id'], current_event.get('player_name'),
                                    current_event.get('jersey_number'),
                                    current_event['source_team_id'], current_event['period']
                                )))

                    if i + 2 < len(df):
                        next_next_event = df.iloc[i + 2]
                        if (next_next_event['source_team_id'] == current_event['source_team_id'] and
                                pd.notna(current_event.get('end_x')) and pd.notna(current_event.get('end_y')) and
                                pd.notna(next_next_event.get('x')) and pd.notna(next_next_event.get('y'))):
                            if (abs(current_event['end_x'] - next_next_event['x']) > 0.01 or
                                    abs(current_event['end_y'] - next_next_event['y']) > 0.01):
                                carries_to_insert.append((i + 2, self._create_carry_row(
                                    current_event, next_next_event,
                                    current_event['end_x'], current_event['end_y'],
                                    next_next_event['x'], next_next_event['y'],
                                    current_event['source_player_id'], current_event.get('player_name'),
                                    current_event.get('jersey_number'),
                                    current_event['source_team_id'], current_event['period']
                                )))
                    continue

            # --- Standard carry insertion ---
            if create_carry and all(pd.notna(v) for v in [carry_start_x, carry_start_y, carry_end_x, carry_end_y]):
                carries_to_insert.append((i + 1, self._create_carry_row(
                    current_event, next_event,
                    carry_start_x, carry_start_y,
                    carry_end_x, carry_end_y,
                    next_event['source_player_id'], next_event.get('player_name'),
                    next_event.get('jersey_number'),
                    next_event['source_team_id'], next_event['period']
                )))

        # Insert carries in reverse order to preserve indices
        df_with_carries = df.copy()
        for idx, carry_row in sorted(carries_to_insert, key=lambda x: x[0], reverse=True):
            df_with_carries = pd.concat([
                df_with_carries.iloc[:idx],
                pd.DataFrame([carry_row]),
                df_with_carries.iloc[idx:]
            ]).reset_index(drop=True)

        print(f"{len(carries_to_insert)} carries insertados en el DataFrame")
        return df_with_carries

    # -------------------------------------------------------------------------
    # EXPECTED THREAT (xT)
    # -------------------------------------------------------------------------

    def _get_xt_value(self, x: Optional[float], y: Optional[float]) -> Optional[float]:
        """
        Devuelve el valor xT de la celda del grid que corresponde a las
        coordenadas reales del campo (X: 0-105, Y: 0-68).

        Args:
            x: Coordenada X en el campo real (0-105)
            y: Coordenada Y en el campo real (0-68)

        Returns:
            Valor xT de esa celda, o None si las coordenadas son inválidas.
        """
        if x is None or y is None or np.isnan(x) or np.isnan(y):
            return None

        col = int(np.clip(x / 105 * 12, 0, 11))
        row = int(np.clip(y / 68  *  8, 0,  7))

        return float(self.XT_GRID[row, col])

    def calculate_xt(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula Expected Threat (xT) para eventos de tipo 'Pass' y 'Carry'.

        Añade tres columnas al DataFrame:
          - start_zone_value_xT : valor xT de la celda de inicio  (x, y)
          - end_zone_value_xT   : valor xT de la celda de fin     (end_x, end_y)
          - xT                  : diferencia end - start

        Args:
            df: DataFrame con todos los eventos (ya con carries si procede)

        Returns:
            DataFrame con las tres nuevas columnas añadidas.
        """
        df = df.copy()

        df['start_zone_value_xt'] = np.nan
        df['end_zone_value_xt']   = np.nan
        df['xt']                  = np.nan

        mask = df['event_type'].isin(['Pass', 'Carry'])

        if not mask.any():
            print("No se encontraron eventos de tipo 'Pass' o 'Carry' para calcular xT")
            return df

        df.loc[mask, 'start_zone_value_xt'] = df.loc[mask].apply(
            lambda row: self._get_xt_value(row['x'], row['y']), axis=1
        )
        df.loc[mask, 'end_zone_value_xt'] = df.loc[mask].apply(
            lambda row: self._get_xt_value(row['end_x'], row['end_y']), axis=1
        )
        df.loc[mask, 'xt'] = (
            df.loc[mask, 'end_zone_value_xt'] - df.loc[mask, 'start_zone_value_xt']
        )

        xt_events   = mask.sum()
        xt_computed = df.loc[mask, 'xt'].notna().sum()
        print(f"xT calculado: {xt_computed}/{xt_events} eventos Pass/Carry con coordenadas válidas")

        return df

    # -------------------------------------------------------------------------
    # MAIN PROCESSING PIPELINE
    # -------------------------------------------------------------------------

    def process_events_file(
        self,
        file_path: str,
        match_id: int,
        include_carries: bool = True,
        include_xt: bool = True
    ) -> pd.DataFrame:
        """
        Procesa un archivo de eventos y devuelve un DataFrame estructurado.

        El orden del pipeline es:
          1. Parsear JSONP y extraer datos
          2. Mapear equipos (team_name, opposition_team_name, h_a)
          3. Mapear jersey numbers
          4. Construir columnas básicas + qualifiers + coordenadas
          5. (Opcional) Calcular e insertar carries
          6. (Opcional) Calcular xT para Pass y Carry
          7. Resolver FKs (team_id, player_id) desde caches de silver

        Args:
            file_path:       Ruta al archivo JSONP de eventos
            match_id:        Internal match_id ya resuelto desde silver.matches
            include_carries: Si True, calcula e inserta carries (default: True)
            include_xt:      Si True, calcula columnas xT para Pass/Carry (default: True)

        Returns:
            DataFrame con los eventos procesados (columnas wide + silver columns)
        """
        try:
            data = self._extract_json_from_jsonp(file_path)

            if 'liveData' not in data or 'event' not in data['liveData']:
                raise ValueError("No se encontraron eventos en el archivo")

            events = data['liveData']['event']

            # Mapeo de equipos (debe hacerse antes de procesar eventos)
            contestants = data.get('matchInfo', {}).get('contestant', [])
            self.team_mapping = self._extract_team_mapping(contestants)

            self.player_jersey_mapping = self._extract_player_jersey_mapping(events)

            processed_events = []
            skipped_unknown = 0

            for event in events:
                row = self._extract_basic_columns(event)
                if row.get('event_type') == 'Unknown':
                    skipped_unknown += 1
                    continue
                processed_events.append(row)

            if skipped_unknown > 0:
                print(f"Se omitieron {skipped_unknown} eventos con tipo 'Unknown'")

            df = pd.DataFrame(processed_events)

            # Stamp match_id on every row
            df['match_id'] = match_id
            self._current_match_id = match_id


            print(f"Procesados {len(df)} eventos exitosamente")
            print(f"   Total columnas: {len(df.columns)}")

            if include_carries:
                df = self.calculate_carries(df)

            if include_xt:
                df = self.calculate_xt(df)

            # Sequence columns — initialised as NULL here.
            # Populated in a separate pass by classify_possession_sequences()
            # before the gold layer aggregate is built.
            df['sequence_id']           = None
            df['sequence_start']        = False
            df['sequence_end']          = False
            df['sequence_event_number'] = None

            # Resolve source FKs → internal IDs (requires caches to be loaded)
            if self._team_id_cache or self._player_id_cache:
                df = self._resolve_fk_columns(df)

            return df

        except Exception as e:
            print(f"Error procesando archivo {file_path}: {e}")
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # BULK LOAD — scan all matches and write to silver.events
    # -------------------------------------------------------------------------

    def load_all_matches(
        self,
        base_path: str,
        conn,
        include_carries: bool = True,
        include_xt: bool = True,
        skip_existing: bool = True,
        dry_run: bool = False,
        file_glob: str = "*"
    ) -> Dict[str, Any]:
        """
        Escanea todos los archivos de eventos bajo base_path, los procesa y
        los inserta en silver.events.

        Estructura de directorios esperada:
            {base_path}/{competition_code}/{season}/matches/{file}

        Cualquier nueva competición o temporada que siga esta estructura
        será recogida automáticamente sin cambios en el código.

        Args:
            base_path:       Raíz del directorio de raw data (e.g. "data/raw")
            conn:            psycopg2 connection abierta por el caller
            include_carries: Calcular e insertar carries (default: True)
            include_xt:      Calcular xT para Pass/Carry (default: True)
            skip_existing:   Si True, omite partidos que ya tienen eventos en silver
            dry_run:         Si True, procesa pero NO escribe en la base de datos
            file_glob:       Patrón de ficheros a buscar (default: "*.json*")

        Returns:
            Resumen del proceso:
            {
                'total_files':   int,
                'loaded':        int,
                'skipped':       int,   # match ya existía en silver
                'failed':        int,
                'failed_files':  list[str]
            }
        """
        base = Path(base_path)

        # Walk the full tree but only collect files directly inside a folder
        # named 'matches/'. The 'squad/' sibling folder is never touched.
        # Pattern resolves to: {base}/**/{competition_code}/{season}/matches/{file}
        match_files = sorted(
            f for f in base.rglob(f"matches/{file_glob}")
            if f.is_file()                # ← ensures we only get files, not the folder itself
        )

        if not match_files:
            print(f"No se encontraron archivos en {base}/**/matches/{file_glob}")
            return {'total_files': 0, 'loaded': 0, 'skipped': 0, 'failed': 0, 'failed_files': []}

        print(f"{len(match_files)} archivos encontrados en {base}")

        # Load FK caches once for the whole run
        self._load_id_caches(conn)

        summary = {
            'total_files': len(match_files),
            'loaded':      0,
            'skipped':     0,
            'failed':      0,
            'failed_files': []
        }

        for file_path in match_files:
            relative = file_path.relative_to(base)
            print(f"\nProcesando: {relative}")

            try:
                # Step 1: extract source_match_id from the file header
                source_match_id = self._extract_source_match_id(str(file_path))
                if not source_match_id:
                    raise ValueError("No se pudo extraer source_match_id del archivo")

                # Step 2: resolve to internal match_id
                match_id = self._resolve_match_id(source_match_id, conn)
                if match_id is None:
                    print(f"source_match_id '{source_match_id}' no encontrado en silver.matches — omitiendo")
                    summary['skipped'] += 1
                    continue

                # Step 3: skip if already loaded
                if skip_existing and self._check_match_already_loaded(match_id, conn):
                    print(f"match_id={match_id} ya existe en silver.events — omitiendo")
                    summary['skipped'] += 1
                    continue

                # Step 4: process the file
                df = self.process_events_file(
                    str(file_path),
                    match_id=match_id,
                    include_carries=include_carries,
                    include_xt=include_xt
                )

                if df.empty:
                    raise ValueError("El procesado devolvió un DataFrame vacío")

                # Step 5: select only silver.events columns and write
                if dry_run:
                    print(f"   [dry-run] Would insert {len(df)} events for match_id={match_id}")
                else:
                    self._insert_to_silver(df, conn)
                    conn.commit()

                summary['loaded'] += 1
                print(f"match_id={match_id} cargado en silver.events ({len(df)} eventos)")

            except Exception as e:
                conn.rollback()
                print(f"❌ Error en {relative}: {e}")
                summary['failed'] += 1
                summary['failed_files'].append(str(relative))

        # Final summary
        print(f"\n{'='*55}")
        print(f"Resumen del proceso:")
        print(f"   Archivos totales : {summary['total_files']}")
        print(f"   Cargados         : {summary['loaded']}")
        print(f"   Omitidos         : {summary['skipped']}")
        print(f"   Fallidos         : {summary['failed']}")
        if summary['failed_files']:
            print(f"   Archivos fallidos:")
            for f in summary['failed_files']:
                print(f"      - {f}")
        print(f"{'='*55}")

        return summary

    def _insert_to_silver(self, df: pd.DataFrame, conn) -> None:
        """
        Selecciona las columnas de silver.events del DataFrame wide y las
        inserta en la tabla usando psycopg2.extras.execute_values — un único
        round-trip por partido, más rápido que to_sql(method='multi').

        Las columnas de qualifiers, xT, y de depuración (source_event_id,
        provider_event_id, player_name, etc.) NO se escriben en silver.

        Args:
            df:   DataFrame procesado (columnas wide)
            conn: psycopg2 connection (commit lo gestiona load_all_matches)
        """
        # Ensure all silver columns exist (some may be absent for edge cases)
        for col in self.SILVER_COLUMNS:
            if col not in df.columns:
                df[col] = None

        silver_df = df[self.SILVER_COLUMNS].copy()

        # Replace pandas NA/NaN with None so psycopg2 maps them to SQL NULL
      # With these three:
        silver_df['timestamp'] = pd.to_datetime(
            silver_df['timestamp'], errors='coerce', utc=True
        )
        silver_df = silver_df.astype(object).where(pd.notnull(silver_df), None)

        cols = ", ".join(self.SILVER_COLUMNS)
        placeholders = "(" + ", ".join(["%s"] * len(self.SILVER_COLUMNS)) + ")"

        rows = [tuple(row) for row in silver_df.itertuples(index=False, name=None)]

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO silver.events ({cols}) VALUES %s",
                rows,
                page_size=500   # flush every 500 rows — balances memory vs round-trips
            )

    # -------------------------------------------------------------------------
    # LOOKUP HELPERS
    # -------------------------------------------------------------------------

    def get_event_info(self, type_id: int) -> Dict[str, str]:
        """
        Obtiene información de un evento por su typeId.

        Args:
            type_id: ID del tipo de evento

        Returns:
            Diccionario con name y description del evento
        """
        return self.event_codes.get(type_id, {'name': 'Unknown', 'description': 'Unknown event type'})

    def get_qualifier_info(self, qualifier_id: int) -> Dict[str, str]:
        """
        Obtiene información de un qualifier por su qualifierId.

        Args:
            qualifier_id: ID del qualifier

        Returns:
            Diccionario con name, value y description del qualifier
        """
        return self.qualifier_codes.get(qualifier_id, {'name': 'Unknown', 'value': '', 'description': 'Unknown qualifier'})