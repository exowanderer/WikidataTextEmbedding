import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from time import time
from tqdm import tqdm


def post_process_embed_df(df, embedder, embed_batchsize=120):
    start = time()
    n_rows = df.index.size
    pbar = tqdm(df.iterrows(), total=n_rows)

    stack_rows = []
    for _, row_ in pbar:
        stack_rows.append(row_)
        if len(stack_rows) == embed_batchsize:
            statements = [row_.statement for row_ in stack_rows]
            inds = [row_.name for row_ in stack_rows]
            embeddings_ = embedder.encode(statements)

            for ind_, embed_ in zip(inds, embeddings_):
                df.at[ind_, 'embedding'] = embed_.tolist()

            # Reset batch
            stack_rows = []

            ratio_done = (n_rows - df['embedding'].isnull().sum()) / n_rows
            pbar.set_description(f'{ratio_done:0.1%}')
            pbar.refresh()  # to show immediately the update

    if stack_rows:
        print(f'Wrapping up trailing {len(stack_rows)} rows.')
        statements = [row_.statement for row_ in stack_rows]
        inds = [row_.name for row_ in stack_rows]
        embeddings_ = embedder.encode(statements)

        for ind_, embed_ in zip(inds, embeddings_):
            df.at[ind_, 'embedding'] = embed_.tolist()

    print(f'Operation took {time() - start:.1f} seconds.')
    return df


if __name__ == '__main__':
    # Initialize the SentenceTransformer model
    embedder = SentenceTransformer(
        "jinaai/jina-embeddings-v2-base-en",
        trust_remote_code=True
    )

    # Set the batch size for embedding
    embed_batchsize = 100

    # Load the DataFrame from a CSV file
    df_100000 = pd.read_csv('wikidata_vectordb_datadump_100000_en.csv')

    # Ensure the 'embedding' column exists and is initialized with None
    # if 'embedding' not in df_100000.columns:
    df_100000['embedding'] = None

    # Process the DataFrame and add embeddings
    df_1e5_embedded = post_process_embed_df(
        df=df_100000,
        embedder=embedder,
        embed_batchsize=embed_batchsize
    )

    outfilename = 'wikidata_vectordb_datadump_100000_embedded_en.csv'
    df_1e5_embedded.to_csv(outfilename, index=False)
