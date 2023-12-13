from toolkit import check_arguments, main_arguments
from uploader import Uploader

if __name__=="__main__":

    args = main_arguments()

    print(args)

    miss_args = check_arguments(args, ['-w', '-f', '-n', '-t'])

    if not len(miss_args):
        u = Uploader(
            website=args.website,
            freq=args.frequency,
            filename=args.name
        )
        u.create_log()
        u.load_history()
        u.upload(args.target)
    
