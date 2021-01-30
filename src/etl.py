from config import config
import helpers
from logger import logger


def main():
    # read config variables
    try:
        USER = config['user']
        PWD = config['pwd']
        HOST = config['host']
        DB = config['db']
        SCHEMA = config['schema']
        TABLE = config['table']
    except KeyError as e:
        logger.error(f'Missing config variable {e}')
        raise

    # read in the data
    logger.info('Downloading and reading data')
    geom = helpers.get_data()

    # create a mesh of the defined resolution
    logger.info('Generating bounding mesh')
    mesh = helpers.generate_mesh(geom, 100)

    # get the intersection between the geom and the mesh
    logger.info('Overlaying geometry with mesh')
    overlayed_mesh = helpers.overlay_mesh(geom, mesh)
    logger.info(f'Restricted mesh created with {len(overlayed_mesh)} cells')

    # generate a list of 5 letter words
    logger.info('Generating words')
    words = helpers.get_words()

    # create three word combinations
    logger.info('Create word combinations')
    num_combos = len(overlayed_mesh)
    combos = helpers.create_word_combos(words, num_combos)

    # create a dataframe of words and cells
    logger.info('Creating spatial dataframe')
    words_geoms_df = helpers.construct_dataframe(overlayed_mesh, combos)

    # create DB engine with SQLAlchemy
    logger.info('Connecting to the database')
    engine = helpers.get_engine(USER, PWD, HOST, DB)

    # insert into database
    logger.info('Inserting data into database')
    try:
        helpers.insert_rows(engine, SCHEMA, TABLE, words_geoms_df)
    except Exception as e:
        logger.error(f'Insertion failed with error {e}')
        raise


if __name__ == '__main__':
    main()
