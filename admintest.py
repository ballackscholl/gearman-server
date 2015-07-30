#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author      a
# created on  2015/7/6
import gearman
if __name__ == '__main__':
    client = gearman.GearmanAdminClient(['127.0.0.1:4730'])
    print client.get_status()
    print client.get_workers()
    client.shutdown()