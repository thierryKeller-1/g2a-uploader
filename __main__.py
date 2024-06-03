from toolkit import check_arguments, main_arguments, UnreconizedParmException
from uploader import Uploader
import sys


required_args = ['website', 'frequency', 'name', 'snapshotdate', 'target']
website_args = ['booking', 'campings', 'maeva', 'edomizil']
frequency_args = [1,3,7]

def validate_args(args:object) -> None:
        global required_args
        args = vars(args)
        if list(args.keys()) != required_args:
            message = f"{list(set(args.keys()).difference(required_args))} arguments are missing or incorrect"
            raise UnreconizedParmException(message)

        for required_arg in required_args:
            arg = args[required_arg].lower()
            match(required_arg):
                case 'website':
                    if arg not in website_args:
                        raise UnreconizedParmException(f"website should be in {website_args}")
                case 'frequency':
                    if arg not in frequency_args:
                        raise UnreconizedParmException(f"frequency should be in {frequency_args}")
                case 'name':
                    if arg == '':
                        raise UnreconizedParmException(f"name should be define")
                case 'snapshotdate':
                    if arg == '':
                        raise UnreconizedParmException(f"snapshotdate should be define")

                    

if __name__=="__main__":

    args = main_arguments()
    validate_args(args)
    
    miss_args = check_arguments(args, ['-w', '-f', '-n', '-d'])

    if not len(miss_args) :
        u = Uploader(
            website=args.website,
            freq=args.frequency,
            filename=args.name,
            target=args.target,
            date_snapshot=args.snapshotdate
        )
        u.create_log()
        u.load_history()
        u.upload()
    
