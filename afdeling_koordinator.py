import pandas as pd
from goldcode.utils.db_downloader import DBDownloader
import pandas as pd
import requests

def get_lat_long(row):
    try:
        # -------- Første forsøg: adresse + postnr --------
        url = "https://api.dataforsyningen.dk/adresser"
        params = {
            "vejnavn": row["adresse"],
            "postnr": str(row["post"])
        }

        response = requests.get(url, params=params)
        data = response.json()

        print('OK')

        if len(data) > 0:
            lon, lat = data[0]["adgangsadresse"]["adgangspunkt"]["koordinater"]
            return lat, lon

        # -------- Fallback: kun postnummer --------
        url_post = "https://api.dataforsyningen.dk/postnumre"
        response_post = requests.get(url_post, params={"nr": str(row["post"])})
        data_post = response_post.json()

        if "visueltcenter" in data_post:
            lon, lat = data_post["visueltcenter"]["koordinater"]
            return lat, lon

    except:
        return None, None

    return None, None


def main(output_file, afdelings_id='./data/afdelings_id.xlsx'):
    try:
        sql = """
            WITH base AS (
                SELECT
                    sel,
                    afd,
                    adresse,
                    postby,
                    COUNT(*) AS antal
                FROM lejemaal
                WHERE adresse IS NOT NULL
                AND postby IS NOT NULL
                GROUP BY sel, afd, adresse, postby
            )
            SELECT b.*
            FROM base b
            JOIN (
                SELECT sel, afd, MAX(antal) AS max_antal
                FROM base
                GROUP BY sel, afd
            ) m
            ON b.sel = m.sel
            AND b.afd = m.afd
            AND b.antal = m.max_antal;


        """
        # Henter afdelingsdata
        db = DBDownloader()
        db.set_default_eg_prod()
        df = db.sql(sql)

        # Forbehandling
        df_afd_id = pd.read_excel(afdelings_id)

        df_merged = pd.merge(df, df_afd_id, "left", left_on=['sel', 'afd'], right_on=['Selskabsnr', 'Afdelingsnr'])

        df_merged["post"] = df_merged["postby"].str.split().str[0]

        df_merged["Importeret"] = '2026-02-14'

        # Requester for lat og long

        print(df_merged.info())

        print(df_merged.head(5))

        
        df_merged[["latitude", "longitude"]] = df_merged.apply(
            lambda row: pd.Series(get_lat_long(row)),
            axis=1
        )

        df_merged.to_excel(output_file, index=False)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    main(output_file = './Afdelingskoordinater.xlsx')