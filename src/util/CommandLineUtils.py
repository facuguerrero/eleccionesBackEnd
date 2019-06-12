from subprocess import call


class CommandLineUtils:

    @staticmethod
    def copy(source, destination):
        call(f'cp {source} {destination}', shell=True)

    @staticmethod
    def move(source, destination):
        call(f'mv {source} {destination}', shell=True)

    @staticmethod
    def remove(objective):
        call(f'rm -rf {objective}', shell=True)

    @staticmethod
    def execute(command, output=False):
        call(f'{command} {"" if output else ">/dev/null 2>&1"}', shell=True)
