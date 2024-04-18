from toolkit import check_arguments, main_arguments
from uploader import Uploader

if __name__=="__main__":

    args = main_arguments()

    print(args)

    miss_args = check_arguments(args, ['-w', '-f', '-n', '-t', '-d'])

    if not len(miss_args):
        u = Uploader(
            website=args.website,
            freq=args.frequency,
            filename=args.name,
            date_snapshot=args.snapshotdate
        )
        u.create_log()
        u.load_history()
        u.upload(args.target)
    
