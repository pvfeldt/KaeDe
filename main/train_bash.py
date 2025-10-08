import os
import sys
path = ".."
abs_path = os.path.abspath(path)
sys.path.append(abs_path)
os.environ["WANDB_DISABLED"] = "true"

from llamafactory.train.tuner import run_exp


def main():
    run_exp()


def _mp_fn(index):
    # For xla_spawn (TPUs)
    main()


if __name__ == "__main__":
    main()
