# -*- coding: utf-8 -*-

class MogileFSError(Exception):
    pass

class NoDomainError(MogileFSError):
    pass

class NoClassError(MogileFSError):
    pass

class NoKeyError(MogileFSError):
    pass
