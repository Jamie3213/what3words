import fiona.crs
import geopandas as gpd
from math import ceil, floor
import random
import requests
from shapely.geometry import box, Point, MultiPolygon


def get_data():
    # get GeoJSON from API
    lad_url = 'https://opendata.arcgis.com/datasets/1d78d47c87df4212b79fe2323aae8e08_0.geojson'
    response = requests.get(url)
    content = response.content

    # read as spatial dataframe
    lad = gpd.read_file(content.decode())
    lad.to_crs('EPSG:27700', inplace=True)

    # extract Bury boundary
    bury_geom = lad[lad.lad19nm == 'Bury']
    return bury_geom.geometry.values[0]


def create_bounding_box(geom, res):
    # utility function to round to a given base
    def round_to_base(num, base, direction):
        if direction == 'up':
            return base * ceil(num / base)
        elif direction == 'down':
            return base * floor(num / base)

    # extract bounding points for the polygon and calculate
    # the width and height of the envelope based on the provided
    # grid resolution
    minx, miny, maxx, maxy = geom.bounds

    width = round_to_base(maxx, base=res, direction='up')
    - round_to_base(minx, base=res, direction='down')

    height = round_to_base(maxy, base=res, direction='up')
    - round_to_base(miny, base=res, direction='down')

    # calculate number of vertical and horizontal cells
    cells_x = int(width / res)
    cells_y = int(height / res)

    # create a mesh
    mesh = []
    for i in range(cells_x):
        for j in range(cells_y):
            cell = box(
                round_to_base(minx, base=res, direction='down')
                + (i * res),
                round_to_base(miny, base=res, direction='down')
                + (j * res),
                round_to_base(minx, base=res, direction='down')
                + ((i + 1) * res),
                round_to_base(miny, base=res, direction='down')
                + ((j + 1) * res))
            mesh.append(cell)

    return mesh


def overlay_mesh(geom, mesh):
    # extract cells which intersect the main polygon
    return [cell for cell in mesh if cell.intersects(geom)]


def get_words():
    # create a list of 5 letter words
    word_url = 'https://www.mit.edu/~ecprice/wordlist.10000'
    all_words = requests.get(word_url).text
    return [word for word in all_words.split('\n') if len(word) == 5]


def create_words_combos(words, num_combos):
    combos = []
    for i in range(num_combos):
        # parse combo
        combo = f'{random.choice(words)}'
        f'.{random.choice(words)}'
        f'.{random.choice(words)}'
        
        # make sure we don't have duplicate combinations
        if combo in combos:
            i -= 1
        else:
            combos.append(combo)


def construct_dataframe(geom_as_mesh, combos):
    what3words = gpd.GeoDataFrame({'geometry': cell} for cell in geom_as_mesh)
    what3words['three_words'] = combos

    # convert projection
    what3words.to_crs('EPSG:4326', inplace=True)
    return what3words


def connect_to_db():
    pass


def write_words(words_df):
    pass
